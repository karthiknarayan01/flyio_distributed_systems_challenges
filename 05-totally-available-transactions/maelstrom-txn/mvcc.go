// The storage engine — the MVCC part. Every key keeps all of its
// versions in one append-only chain, and one lock per key serializes
// whoever touches that chain. Versions are never deleted or rewritten
// in place; whether a version may be read is decided purely by the
// commit log (clog.go), so uncommitted work simply stays invisible
// until it commits.

package main

import "sync"

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
// see, or nil when nothing is visible yet. The transaction already
// holds the key's lock — every key it touches is locked up front — so
// its row chain can be read here alone.
func readVisible(key int, txn txnCtx) any {
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

// keysLock guards the keyLocks map itself; the map holds one lock per
// key, so a read and a write of the same key never overlap.
var (
	keysLock sync.Mutex
	keyLocks = map[int]*sync.Mutex{}
)

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
// lockTxnKeys and kept until commit — so its row chain can be read
// and extended here alone.
func writeVersion(key, value int, txn txnCtx) {
	raw, _ := keyRows.Load(key)
	rows, _ := raw.([]*row)
	if len(rows) > 0 {
		rows[len(rows)-1].xmax = txn.xid
	}
	keyRows.Store(key, append(rows, &row{value: value, ts: txn.ts, xmin: txn.xid}))
}
