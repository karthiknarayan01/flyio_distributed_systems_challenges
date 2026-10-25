package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node

	// keysLock guards the locks map itself; the map holds one lock per
	// key, so a read and a write of the same key never overlap.
	keysLock sync.Mutex
	keyLocks = map[int]*sync.Mutex{}
)

// sweepPause is how often we re-send WAL records waiting for peer acks.
const sweepPause = 120 * time.Millisecond

func main() {
	node = maelstrom.NewNode()

	node.Handle("txn", handleTxn)
	node.Handle("replicate", handleReplicate)

	go runSweep()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}

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
	lockWriteKeys(writeKeys(ops), txn)

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

// writeKeys answers the distinct keys of the write operations, sorted
// ascending; the sort is what makes transactions that grab the same
// keys wait behind each other in one line, never in circles.
func writeKeys(ops []op) []int {
	seen := map[int]bool{}
	var keys []int
	for _, op := range ops {
		if op.name == "w" && !seen[op.key] {
			seen[op.key] = true
			keys = append(keys, op.key)
		}
	}
	sort.Ints(keys)
	return keys
}

// lockWriteKeys holds each write key's lock for the transaction's whole
// run, taking them one by one in the sorted order above. Every other
// transaction touching those keys queues behind it and follows once it
// commits.
func lockWriteKeys(keys []int, txn txnCtx) {
	for _, key := range keys {
		lock := lockFor(key)
		lock.Lock()
		txn.locks[key] = lock
	}
}

// A transaction walks two commit-log states: in-progress while its
// writes land, committed once they become visible.
const (
	inProgress = iota
	committed
)

// clog is the commit log: one status per xid. Reads and writes consult
// it to decide which row versions count as committed.
var (
	clogLock sync.Mutex
	clog     = map[string]int{}
)

// setStatus records one transition of a transaction's commit-log
// status.
func setStatus(xid string, status int) {
	clogLock.Lock()
	clog[xid] = status
	clogLock.Unlock()
}

// isCommitted answers whether a transaction reached the committed
// state.
func isCommitted(xid string) bool {
	clogLock.Lock()
	defer clogLock.Unlock()
	return clog[xid] == committed
}

// row is one version of a key's value: xmin created it, xmax (when
// set) names the transaction that replaced it.
type row struct {
	value int
	ts    int64
	xmin  string
	xmax  string
}

// keyRows holds every version ever written per key; entries are never
// removed, and the newest visible version answers each read.
var keyRows sync.Map // int -> []*row

// readVisible answers the newest version of a key this transaction may
// see, or nil when nothing is visible yet. A key the transaction
// locked for writing is already held, so its lock is skipped then.
func readVisible(key int, txn txnCtx) any {
	lock, held := txn.locks[key]
	if !held {
		lock = lockFor(key)
		lock.Lock()
		defer lock.Unlock()
	}

	raw, _ := keyRows.Load(key)
	rows, _ := raw.([]*row)

	var best *row
	for _, cur := range rows {
		if visible(cur, txn.xid) && (best == nil || newerThan(cur, best)) {
			best = cur
		}
	}

	if best == nil {
		return nil
	}
	return best.value
}

// visible applies the commit log's visibility rule to one version: our
// own version counts unless we replaced it later; any other version
// counts when its creator committed and its replacer, if any, has not
// committed yet.
func visible(cur *row, self string) bool {
	if cur.xmin == self {
		return cur.xmax != self
	}
	return isCommitted(cur.xmin) && (cur.xmax == "" || !isCommitted(cur.xmax))
}

// newerThan answers whether version a sorts after b: later timestamp
// wins, and the zero-padded xid breaks exact ties lexically.
func newerThan(a, b *row) bool {
	return a.ts > b.ts || (a.ts == b.ts && a.xmin > b.xmin)
}

// lockFor returns a key's lock, created on first use.
func lockFor(key int) *sync.Mutex {
	keysLock.Lock()
	lock, ok := keyLocks[key]
	if !ok {
		lock = &sync.Mutex{}
		keyLocks[key] = lock
	}
	keysLock.Unlock()
	return lock
}

// writeVersion marks the head version replaced and appends its own.
// The transaction already holds the key's lock — taken up front by
// lockWriteKeys and kept until commit — so its row chain can be read
// and extended here alone.
func writeVersion(key, value int, txn txnCtx) {
	raw, _ := keyRows.Load(key)
	rows, _ := raw.([]*row)
	if len(rows) > 0 {
		rows[len(rows)-1].xmax = txn.xid
	}
	keyRows.Store(key, append(rows, &row{value: value, ts: txn.ts, xmin: txn.xid}))
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

// walRecord is one committed transaction queued for replay on every
// peer until they ack it.
type walRecord struct {
	id     int
	xid    string
	ts     int64
	writes [][2]int
}

// pending maps each peer to the WAL records it has not acked yet. The
// sweeper re-sends these, so a partition drops frames but never a
// committed transaction.
var (
	pendingLock sync.Mutex
	pending     = map[string]map[int]*walRecord{}
)

// enqueueReplica queues one committed transaction against every peer,
// keeping only each key's final write, since earlier ones are
// intermediates no reader may observe. Replicas only receive: the
// author already shipped it to all of them.
func enqueueReplica(txn txnCtx, writes [][2]int) {
	fin := finalWrites(writes)
	if len(fin) == 0 {
		return
	}
	record := &walRecord{id: txn.id, xid: txn.xid, ts: txn.ts, writes: fin}
	pendingLock.Lock()
	for _, peer := range peers() {
		if pending[peer] == nil {
			pending[peer] = map[int]*walRecord{}
		}
		pending[peer][record.id] = record
	}
	pendingLock.Unlock()
}

// finalWrites keeps each key's last write, in first-write key order.
func finalWrites(writes [][2]int) [][2]int {
	values := map[int]int{}
	var keyOrder []int
	for _, w := range writes {
		if _, seen := values[w[0]]; !seen {
			keyOrder = append(keyOrder, w[0])
		}
		values[w[0]] = w[1]
	}
	final := make([][2]int, 0, len(keyOrder))
	for _, key := range keyOrder {
		final = append(final, [2]int{key, values[key]})
	}
	return final
}

// peers answers the node ids other than ours.
func peers() []string {
	peerIDs := []string{}
	for _, id := range node.NodeIDs() {
		if id != node.ID() {
			peerIDs = append(peerIDs, id)
		}
	}
	return peerIDs
}

// handleReplicate replays committed transactions from other nodes:
// their commit-log status is recorded, their versions appended, and
// the ids acked so the author's sweeper stops resending them.
func handleReplicate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ids := []any{}
	for _, rawEvent := range body["txns"].([]any) {
		event := rawEvent.(map[string]any)
		xid := event["xid"].(string)
		ts := int64(event["ts"].(float64))
		setStatus(xid, committed)
		for _, rawWrite := range event["writes"].([]any) {
			pair := rawWrite.([]any)
			appendVersion(int(pair[0].(float64)), int(pair[1].(float64)), ts, xid)
		}
		ids = append(ids, event["id"])
	}

	return node.Reply(msg, map[string]any{"type": "replicate_ok", "ids": ids})
}

// appendVersion replays one committed write as a fresh version. It
// only ever adds: a re-delivered frame copies an equal version that
// never wins a read, so retries need no dedupe.
func appendVersion(key, value int, ts int64, xid string) {
	lock := lockFor(key)
	lock.Lock()
	raw, _ := keyRows.Load(key)
	rows, _ := raw.([]*row)
	keyRows.Store(key, append(rows, &row{value: value, ts: ts, xmin: xid}))
	lock.Unlock()
}

// maxRecordsPerFrame caps how many WAL records ride in one replicate
// frame: a long partition backs up thousands of records, and one huge
// frame would overflow the peer's stdin reader (a 64KB bufio limit).
const maxRecordsPerFrame = 100

// runSweep re-sends every peer's unacked WAL records, split into
// bounded frames, until the acks come back.
func runSweep() {
	ticker := time.NewTicker(sweepPause)
	defer ticker.Stop()
	for range ticker.C {
		pendingLock.Lock()
		batches := map[string][][]any{}
		for peer, records := range pending {
			if len(records) == 0 {
				continue
			}
			batch := []any{}
			for _, record := range records {
				batch = append(batch, recordPayload(record))
				if len(batch) == maxRecordsPerFrame {
					batches[peer] = append(batches[peer], batch)
					batch = []any{}
				}
			}
			if len(batch) > 0 {
				batches[peer] = append(batches[peer], batch)
			}
		}
		pendingLock.Unlock()

		for peer, chunks := range batches {
			for _, chunk := range chunks {
				sendReplica(peer, map[string]any{"type": "replicate", "txns": chunk})
			}
		}
	}
}

// recordPayload builds the JSON shape of one WAL record.
func recordPayload(record *walRecord) map[string]any {
	writes := []any{}
	for _, w := range record.writes {
		writes = append(writes, []any{w[0], w[1]})
	}
	return map[string]any{
		"id":     record.id,
		"xid":    record.xid,
		"ts":     record.ts,
		"writes": writes,
	}
}

// sendReplica fires one batched frame at a peer and clears whatever it
// acks. A dropped frame acks nothing, so the next sweep carries its
// records again.
func sendReplica(peer string, frame map[string]any) {
	err := node.RPC(peer, frame, func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		pendingLock.Lock()
		for _, raw := range body["ids"].([]any) {
			delete(pending[peer], int(raw.(float64)))
		}
		pendingLock.Unlock()
		return nil
	})
	if err != nil {
		log.Print(err)
	}
}
