// Replication — the WAL path. After a commit, its writes are queued as
// one WAL record per peer, and a background sweeper re-sends unacked
// records until every peer acks, so a partition can delay a
// transaction's arrival but never lose it. Replicas replay each record
// into their version chain and mark it committed as they go; they only
// ever receive, because the author already ships the record to every
// node itself.

package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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

// sweepPause is how often we re-send WAL records waiting for peer acks.
const sweepPause = 120 * time.Millisecond

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
