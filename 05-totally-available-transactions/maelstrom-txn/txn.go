// A transaction's life on the node that received it: parse the
// client's operations, lock every key it touches up front in sorted
// order, run the operations one by one, commit, queue the WAL record,
// and answer. One goroutine runs each transaction; it leans on the
// storage in mvcc.go and the commit log in clog.go.

package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// handleTxn runs one client transaction, Postgres style: it locks all
// of its write keys up front, keys sorted, so competing transactions
// always queue behind it instead of fighting for the same keys; writes
// land as new row versions that stay invisible until the commit log
// marks them committed, and the WAL entry is queued before the client
// is answered, so a reply is never sent for uncommitted work.
func handleTxn(msg maelstrom.Message) error {
	waitForInit()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ops := parseOps(body["txn"].([]any))
	txn := beginTxn()
	lockTxnKeys(txnKeys(ops), txn)

	out := make([]any, 0, len(ops))
	var writes [][2]int
	for _, op := range ops {
		switch op.name {
		case "r":
			out = append(out, []any{op.name, op.key, readVisible(op.key, txn)})
		case "w":
			writeVersion(op.key, op.value, txn)
			out = append(out, []any{op.name, op.key, op.value})
			writes = append(writes, [2]int{op.key, op.value})
		}
	}

	commitTxn(txn)
	enqueueReplica(txn, writes)

	return node.Reply(msg, map[string]any{"type": "txn_ok", "txn": out})
}

// op is one of the micro-operations a transaction carries: a read or a
// write of one key.
type op struct {
	name  string
	key   int
	value int
}

// parseOps turns the raw operation lists of a txn frame into typed
// operations once, so the transaction body works on plain values.
func parseOps(rawOps []any) []op {
	ops := make([]op, 0, len(rawOps))
	for _, rawOp := range rawOps {
		fields := rawOp.([]any)
		value, _ := fields[2].(float64)
		ops = append(ops, op{
			name:  fields[0].(string),
			key:   int(fields[1].(float64)),
			value: int(value),
		})
	}
	return ops
}

// waitForInit: our own id and the peer list arrive with the init
// message, and both are needed to mint xids and queue WAL records.
func waitForInit() {
	for node.ID() == "" {
		time.Sleep(time.Millisecond)
	}
}

// txnCtx carries one transaction's identity for its whole run: the
// commit-log id, the sweeper's ack id, the timestamp that orders
// concurrent versions, and the write-key locks held until commit.
type txnCtx struct {
	xid   string
	id    int
	ts    int64
	locks map[int]*sync.Mutex
}

// txnCounter mints the numeric half of our xids.
var txnCounter int64

// beginTxn opens a transaction: a globally distinct xid, zero-padded so
// the text sorts by number, entered in the commit log as in-progress
// where readers can check it.
func beginTxn() txnCtx {
	n := int(atomic.AddInt64(&txnCounter, 1))
	txn := txnCtx{
		xid:   node.ID() + ":" + fmt.Sprintf("%08d", n),
		id:    n,
		ts:    time.Now().UnixMilli(),
		locks: map[int]*sync.Mutex{},
	}
	setStatus(txn.xid, inProgress)
	return txn
}

// txnKeys answers the distinct keys of all the transaction's
// operations, reads included, sorted ascending. Sorting makes
// transactions that overlap on keys queue behind each other in one
// line, never in circles, and locking read keys up front means a
// running transaction never waits for a fresh lock, so no two can end
// up each holding what the other needs.
func txnKeys(ops []op) []int {
	seen := map[int]bool{}
	var keys []int
	for _, op := range ops {
		if !seen[op.key] {
			seen[op.key] = true
			keys = append(keys, op.key)
		}
	}
	sort.Ints(keys)
	return keys
}

// lockTxnKeys holds each key's lock for the transaction's whole run,
// taking them one by one in the sorted order above. Every other
// transaction touching those keys queues behind it and follows once it
// commits.
func lockTxnKeys(keys []int, txn txnCtx) {
	for _, key := range keys {
		lock := lockFor(key)
		lock.Lock()
		txn.locks[key] = lock
	}
}

// commitTxn marks the transaction committed — its versions become
// visible — and then frees the write-key locks it held, letting the
// next transaction in line through.
func commitTxn(txn txnCtx) {
	setStatus(txn.xid, committed)
	for _, lock := range txn.locks {
		lock.Unlock()
	}
}
