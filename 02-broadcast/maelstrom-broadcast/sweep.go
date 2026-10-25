// The sending half of gossip. Numbers a broadcast queued for a
// neighbor ride in one batched frame every sweep until that neighbor
// acks; the ack clears the resend list and records what the neighbor
// has. A partition drops frames, never numbers.

package main

import (
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// sweepPause is how often we re-send numbers waiting for a peer ack.
const sweepPause = 120 * time.Millisecond

// awaitingAck maps a neighbor id to the numbers we sent it but it has
// not acked yet. The sweep keeps re-sending these until the ack comes.
var awaitingAck = map[string]map[int]struct{}{}

// markSent adds numbers to a neighbor's awaitingAck set.
func markSent(neighbor string, numbers []int) {
	lock.Lock()
	set, ok := awaitingAck[neighbor]
	if !ok {
		set = map[int]struct{}{}
		awaitingAck[neighbor] = set
	}
	for _, m := range numbers {
		set[m] = struct{}{}
	}
	lock.Unlock()
}

// runSweep sends all pending, every sweepPause: one batched frame per
// neighbor with everything it has not acked yet. Batched sending keeps
// the frame and ack count down, and under a partition the frame is
// dropped and the numbers stay in awaitingAck; once the network heals,
// the ack clears them.
func runSweep() {
	ticker := time.NewTicker(sweepPause)
	defer ticker.Stop()
	for range ticker.C {
		batches := map[string][]int{}
		lock.Lock()
		for neighbor, set := range awaitingAck {
			if len(set) == 0 {
				continue
			}
			numbers := []int{}
			for m := range set {
				numbers = append(numbers, m)
			}
			batches[neighbor] = numbers
		}
		lock.Unlock()
		for neighbor, numbers := range batches {
			sendFrame(neighbor, numbers)
		}
	}
}

// sendFrame fires one frame with a list of numbers to a neighbor. The
// numbers stay in awaitingAck until the neighbor acks; the ack also puts
// them in the neighbor's set. RPC is async, so we never wait for it here.
func sendFrame(neighbor string, numbers []int) {
	err := node.RPC(neighbor,
		map[string]any{"type": "broadcast", "messages": numbers},
		func(msg maelstrom.Message) error {
			lock.Lock()
			for _, m := range numbers {
				delete(awaitingAck[neighbor], m)
				numberSet(neighbor)[m] = struct{}{}
			}
			lock.Unlock()
			return nil
		})
	if err != nil {
		log.Print(err)
	}
}
