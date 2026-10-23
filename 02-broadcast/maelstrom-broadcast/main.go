package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node
	lock sync.Mutex

	// hasNumbers maps a node id to the numbers that node has, as far as we
	// know. Our own id is a key in it, and that set is what read returns.
	hasNumbers = map[string]map[int]struct{}{}

	// awaitingAck maps a neighbor id to the numbers we sent it but it has
	// not acked yet. The sweep keeps re-sending these until the ack comes.
	awaitingAck = map[string]map[int]struct{}{}

	// topology is our copy of the topology message.
	topology map[string][]string
)

// Handlers can run before the init message was applied, and our data is
// keyed by node.ID(), so wait until init is done.
func waitForInit() {
	for node.ID() == "" {
		time.Sleep(time.Millisecond)
	}
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

// numberSet returns (and creates) the set for a node id. Caller holds lock.
func numberSet(id string) map[int]struct{} {
	set, ok := hasNumbers[id]
	if !ok {
		set = map[int]struct{}{}
		hasNumbers[id] = set
	}
	return set
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

// forwardTargets returns who to forward numbers to: our topology row, or
// every other node when the topology has not arrived yet. Caller holds lock.
func forwardTargets(sender string) []string {
	targets := topology[node.ID()]
	if len(targets) == 0 {
		for _, id := range node.NodeIDs() {
			if id != node.ID() && id != sender {
				targets = append(targets, id)
			}
		}
	}
	return targets
}

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

// runSweep re-sends, every 500ms, each neighbor's unacked numbers in one
// batched frame. Under a partition the frame is dropped and the numbers
// stay in awaitingAck; once the network heals, the ack clears them.
func runSweep() {
	ticker := time.NewTicker(500 * time.Millisecond)
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
	for _, neighbor := range forwardTargets(msg.Src) {
		missing := missingFrom(neighbor, fresh)
		if len(missing) > 0 {
			sendPlan[neighbor] = missing
		}
	}
	lock.Unlock()

	// Queue the frame for retry and fire a first send right away.
	for neighbor, nums := range sendPlan {
		neighbor := neighbor
		markSent(neighbor, nums)
		sendFrame(neighbor, nums)
	}

	return node.Reply(msg, map[string]any{"type": "broadcast_ok"})
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

func handleTopology(msg maelstrom.Message) error {
	waitForInit()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if rows, ok := body["topology"].(map[string]any); ok {
		parsed := map[string][]string{}
		for id, row := range rows {
			neighbors := []string{}
			if list, ok := row.([]any); ok {
				for _, v := range list {
					if neighbor, ok := v.(string); ok {
						neighbors = append(neighbors, neighbor)
					}
				}
			}
			parsed[id] = neighbors
		}
		lock.Lock()
		topology = parsed
		lock.Unlock()
	}

	return node.Reply(msg, map[string]any{"type": "topology_ok"})
}

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
