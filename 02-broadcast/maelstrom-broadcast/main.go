// Command maelstrom-broadcast implements the broadcast challenge: every
// number a client gives any node must eventually reach every node, and
// the test also grades how few frames and how little latency that
// takes.
//
// The approach:
//
//   - A node records everything and answers immediately: a broadcast is
//     stored, acked, and queued for every neighbor still missing it
//     (gossip.go). Nothing waits on a peer before the client is
//     answered, and reads come from our own set alone.
//   - The nodes agree on a star without needing the suggested
//     topology: the lowest id is the hub, the hub relays for everyone,
//     and everyone else sends only to the hub (topology.go). A star
//     keeps every number two hops from every node.
//   - Sending is batched and retry-until-acked: each sweep puts every
//     neighbor's pending numbers in one frame, and the ack both clears
//     the resend list and tells us the neighbor has the numbers
//     (sweep.go). A partition drops frames but never numbers.
package main

import (
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node

	// lock guards all the gossip bookkeeping: the per-node knowledge in
	// gossip.go and the pending work in sweep.go share one mutex, so
	// planning under it never blocks a call out.
	lock sync.Mutex
)

func main() {
	node = maelstrom.NewNode()

	node.Handle("broadcast", handleBroadcast)
	node.Handle("read", handleRead)
	node.Handle("topology", handleTopology)

	go runSweep()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
