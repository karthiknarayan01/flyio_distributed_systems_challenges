package main

import (
	"encoding/json"
	"log"
	"os"
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

// sweepPause is how often we re-send transactions waiting for peer
// acks.
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

// handleTxn runs a client transaction entirely on our own state and
// answers immediately; the writes are queued for the other nodes
// afterwards. Total availability comes from never waiting for a peer
// before replying.
func handleTxn(msg maelstrom.Message) error {
	waitForInit()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var (
		ts     = time.Now().UnixMilli()
		author = node.ID()
		out    = make([]any, 0, len(body["txn"].([]any)))
		writes [][2]int
	)

	for _, rawOp := range body["txn"].([]any) {
		op := rawOp.([]any)
		name := op[0].(string)
		key := int(op[1].(float64))

		switch name {
		case "r":
			out = append(out, []any{name, key, readLocal(key)})
		case "w":
			value := int(op[2].(float64))
			applyWrite(key, value, ts, author)
			out = append(out, []any{name, key, value})
			writes = append(writes, [2]int{key, value})
		}
	}

	enqueueReplica(ts, author, writes)

	return node.Reply(msg, map[string]any{"type": "txn_ok", "txn": out})
}

// waitForInit: our own id and the peer list arrive with the init
// message, and we need both to stamp and queue transactions.
func waitForInit() {
	for node.ID() == "" {
		time.Sleep(time.Millisecond)
	}
}

// keyEntry is one key's stored value together with the stamp of the
// write that put it there.
type keyEntry struct {
	value  int
	ts     int64
	author string
}

// values holds one entry per key, accessed concurrently by different
// keys, so its map itself stays safe.
var values sync.Map

// readLocal answers a key's stored value, or nil when the key was
// never written.
func readLocal(key int) any {
	lock := lockFor(key)
	lock.Lock()
	raw, ok := values.Load(key)
	lock.Unlock()
	if !ok {
		return nil
	}
	return raw.(*keyEntry).value
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

// applyWrite stores a write when its stamp sorts after the key's
// current one, so a late-arriving old write can never erase a newer
// value. Local writes are always the newest we hold, so they always
// apply.
func applyWrite(key, value int, ts int64, author string) {
	lock := lockFor(key)
	lock.Lock()
	raw, ok := values.Load(key)
	if !ok || stampAfter(ts, author, raw.(*keyEntry)) {
		values.Store(key, &keyEntry{value, ts, author})
	}
	lock.Unlock()
}

// stampAfter answers whether one stamp sorts after the entry's: larger
// milliseconds first, node id as the tiebreak.
func stampAfter(ts int64, author string, entry *keyEntry) bool {
	return ts > entry.ts || (ts == entry.ts && author > entry.author)
}

// pendingTxn is one authored transaction waiting for the other nodes'
// acks.
type pendingTxn struct {
	id     int
	ts     int64
	author string
	writes [][2]int
}

// pending maps each peer to the transactions it has not acked yet. The
// sweeper re-sends these, so a partition drops frames but never a
// transaction.
var (
	pendingLock sync.Mutex
	pending     = map[string]map[int]*pendingTxn{}
	txnCounter  int64
)

// enqueueReplica records one authored transaction against every peer,
// for the sweeper to carry.
func enqueueReplica(ts int64, author string, writes [][2]int) {
	if len(writes) == 0 {
		return
	}
	txn := &pendingTxn{
		id:     int(atomic.AddInt64(&txnCounter, 1)),
		ts:     ts,
		author: author,
		writes: writes,
	}
	pendingLock.Lock()
	for _, peer := range peers() {
		if pending[peer] == nil {
			pending[peer] = map[int]*pendingTxn{}
		}
		pending[peer][txn.id] = txn
	}
	pendingLock.Unlock()
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

// handleReplicate applies transactions authored by other nodes and acks
// the ids it took in, so their sweeper stops re-sending them.
func handleReplicate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ids := []any{}
	for _, rawTxn := range body["txns"].([]any) {
		txn := rawTxn.(map[string]any)
		ts := int64(txn["ts"].(float64))
		author := txn["author"].(string)
		for _, rawWrite := range txn["writes"].([]any) {
			pair := rawWrite.([]any)
			applyWrite(int(pair[0].(float64)), int(pair[1].(float64)), ts, author)
		}
		ids = append(ids, txn["id"])
	}

	return node.Reply(msg, map[string]any{"type": "replicate_ok", "ids": ids})
}

// runSweep re-sends every peer's unacked transactions as one batched
// frame each, until the acks come back.
func runSweep() {
	ticker := time.NewTicker(sweepPause)
	defer ticker.Stop()
	for range ticker.C {
		pendingLock.Lock()
		frames := map[string]map[string]any{}
		for peer, txns := range pending {
			if len(txns) == 0 {
				continue
			}
			list := []any{}
			for _, txn := range txns {
				writes := []any{}
				for _, w := range txn.writes {
					writes = append(writes, []any{w[0], w[1]})
				}
				list = append(list, map[string]any{
					"id":     txn.id,
					"ts":     txn.ts,
					"author": txn.author,
					"writes": writes,
				})
			}
			frames[peer] = map[string]any{"type": "replicate", "txns": list}
		}
		pendingLock.Unlock()

		for peer, frame := range frames {
			sendReplica(peer, frame)
		}
	}
}

// sendReplica fires one batched frame at a peer and clears whatever it
// acks. A dropped frame acks nothing, so the next sweep carries its
// transactions again.
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
