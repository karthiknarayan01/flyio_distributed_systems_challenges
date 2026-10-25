// The knowledge half of gossip: which node has which numbers. A
// broadcast stores and acks first, then queues itself for every
// neighbor still missing it; reads answer from our own set alone.

package main

import (
	"encoding/json"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// hasNumbers maps a node id to the numbers that node has, as far as we
// know. Our own id is a key in it, and that set is what read returns.
var hasNumbers = map[string]map[int]struct{}{}

// waitForInit: our data is keyed by node.ID(), so wait until init is
// done.
func waitForInit() {
	for node.ID() == "" {
		time.Sleep(time.Millisecond)
	}
}

func handleBroadcast(msg maelstrom.Message) error {
	waitForInit()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	numbers := numbersFromBody(body)

	// Under the lock: store what is new and pick what each neighbor is
	// missing. Sending happens outside, so the lock never waits on a call.
	lock.Lock()
	fresh := storeNumbers(msg.Src, numbers)
	sendPlan := map[string][]int{}
	for _, neighbor := range ourNeighbors() {
		missing := missingFrom(neighbor, fresh)
		if len(missing) > 0 {
			sendPlan[neighbor] = missing
		}
	}
	lock.Unlock()

	// Queue the numbers for the sweep. No instant send here: collecting
	// a short window first lets one frame carry several numbers, which
	// keeps the per-number frame and ack cost down.
	for neighbor, nums := range sendPlan {
		markSent(neighbor, nums)
	}

	return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

// numbersFromBody pulls the broadcast numbers out of one frame body.
// Clients send one number under "message"; our retry frames between
// nodes carry a list under "messages".
func numbersFromBody(body map[string]any) []int {
	numbers := []int{}
	if f, ok := body["message"].(float64); ok {
		numbers = append(numbers, int(f))
	}
	if list, ok := body["messages"].([]any); ok {
		for _, v := range list {
			if f, ok := v.(float64); ok {
				numbers = append(numbers, int(f))
			}
		}
	}
	return numbers
}

// storeNumbers adds the numbers we have not seen before to our own set,
// and notes that the sender has all of them (it just sent them to us).
// It returns the numbers that were new to us. Caller holds lock.
func storeNumbers(sender string, numbers []int) []int {
	newToUs := []int{}
	for _, m := range numbers {
		numberSet(sender)[m] = struct{}{}
		if _, seen := numberSet(node.ID())[m]; seen {
			continue
		}
		numberSet(node.ID())[m] = struct{}{}
		newToUs = append(newToUs, m)
	}
	return newToUs
}

// numberSet returns (and creates) the set for a node id. Caller holds lock.
func numberSet(id string) map[int]struct{} {
	set, ok := hasNumbers[id]
	if !ok {
		set = map[int]struct{}{}
		hasNumbers[id] = set
	}
	return set
}

// missingFrom returns the numbers from newToUs that the neighbor's set
// does not have yet. Caller holds lock.
func missingFrom(neighbor string, newToUs []int) []int {
	neighborSet := numberSet(neighbor)
	missing := []int{}
	for _, m := range newToUs {
		if _, has := neighborSet[m]; !has {
			missing = append(missing, m)
		}
	}
	return missing
}

func handleRead(msg maelstrom.Message) error {
	waitForInit()

	// Copy our own set under the lock so the reply never races the
	// broadcast handler.
	lock.Lock()
	numbers := make([]int, 0, len(hasNumbers[node.ID()]))
	for m := range hasNumbers[node.ID()] {
		numbers = append(numbers, m)
	}
	lock.Unlock()

	return node.Reply(msg, map[string]any{"type": "read_ok", "messages": numbers})
}
