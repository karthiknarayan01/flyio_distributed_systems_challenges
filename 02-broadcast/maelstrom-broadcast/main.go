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

// waitForInit: our data is keyed by node.ID(), so wait until init is done.
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

// ourNeighbors returns who we forward numbers to. Our network is a star:
// the hub sends to everyone, each other node only sends to the hub.
// Every node knows this list by itself, so we ignore the topology
// Maelstrom suggests.
func ourNeighbors() []string {
	if node.ID() == hubOf() {
		others := []string{}
		for _, id := range node.NodeIDs() {
			if id != node.ID() {
				others = append(others, id)
			}
		}
		return others
	}
	return []string{hubOf()}
}

// hubOf returns the node that relays every broadcast in our star
// topology. Every node picks the same one: the lowest node id.
func hubOf() string {
	hub := ""
	for _, id := range node.NodeIDs() {
		if hub == "" || id < hub {
			hub = id
		}
	}
	return hub
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
	// Maelstrom still sends its suggested topology; we only ack it and
	// keep our own star neighbor list.
	return node.Reply(msg, map[string]any{"type": "topology_ok"})
}

// runSweep sends all pending, every 120ms: one batched frame per
// neighbor with everything it has not acked yet. Batched sending keeps
// the frame and ack count down, and under a partition the frame is
// dropped and the numbers stay in awaitingAck; once the network heals,
// the ack clears them.
func runSweep() {
	ticker := time.NewTicker(120 * time.Millisecond)
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
