// The commit log: one status per transaction id (xid). It is the
// single source of truth for "has this transaction committed yet" —
// every visibility check consults it, here on the authoring node and
// on replicas, where replayed transactions arrive already committed.

package main

import "sync"

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
