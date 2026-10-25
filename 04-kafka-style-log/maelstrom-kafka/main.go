// Command maelstrom-kafka implements the Kafka-style log (challenges
// 5a-5c): send appends a message to a named log, poll drains from an
// offset, and clients manage per-log commit markers.
//
// The approach:
//
//   - Every log is owned by one node, picked deterministically by
//     hashing its name over the sorted node ids (ownership.go). All
//     log writes happen on the owner, so offsets are dense and grow
//     monotonically without any compare-and-swap on log data, and a
//     send acks from memory alone.
//   - The four message handlers (handlers.go) follow one shape: logs
//     we own are answered locally, anything else is relayed to its
//     owner and that owner's answer comes back to the client.
//   - A log's data lives in its owner's memory (log.go) and is copied
//     into the linear KV store every 250ms by a background flusher
//     (flusher.go), one plain write per log since exactly one node —
//     its owner — ever writes its key.
package main

import (
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node
	kv   *maelstrom.KV
)

// One store or relay call gets this long to answer, then the caller
// tries again.
const rpcTimeout = 500 * time.Millisecond

// retryPause waits after a failed attempt so retries do not hammer the
// peer.
const retryPause = 10 * time.Millisecond

func main() {
	node = maelstrom.NewNode()
	kv = maelstrom.NewLinKV(node)

	node.Handle("send", handleSend)
	node.Handle("poll", handlePoll)
	node.Handle("commit_offsets", handleCommitOffsets)
	node.Handle("list_committed_offsets", handleListCommittedOffsets)

	go runFlusher()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
