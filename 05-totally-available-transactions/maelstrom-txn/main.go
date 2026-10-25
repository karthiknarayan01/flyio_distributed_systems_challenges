// Command maelstrom-txn implements a totally available, read-committed
// transactional key/value store in the Postgres MVCC style, for the
// Maelstrom txn-rw-register workload.
//
// The big picture, in the order data flows through it:
//
//   - A client transaction arrives at any node, and that node serves it
//     alone: it locks every key the transaction touches (keys sorted,
//     so contenders queue rather than deadlock), writes new row
//     versions, commits them in the commit log, ships them to every
//     other node as a WAL record, and only then answers the client.
//     No reply ever describes uncommitted work.
//   - Every node keeps the whole store: a version chain per key
//     (mvcc.go) guarded by one lock per key, and a commit-log status
//     per transaction id (clog.go). A version is readable only when
//     its creator committed and its replacement, if any, has not — so
//     uncommitted work can never be observed by another transaction.
//   - Committed writes reach the other nodes as WAL records replayed
//     until acked (wal.go), so a network partition may delay a
//     transaction's arrival but never lose it.
//
// txn.go holds the client-facing life of a transaction between those
// pieces.
package main

import (
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// node is the Maelstrom node this process runs as; handlers use it to
// answer clients, to name our xids and to reach the peers.
var node *maelstrom.Node

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
