package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// known maps a node id to the set of numbers we believe that node has.
// Our own id is a key in it: that set is what read returns.
// pending maps a neighbor id to the numbers we sent it but it has not
// acked yet; the sweep keeps re-sending those until the ack arrives.
// A neighbor's known set grows two ways: on an ack, and when they send us
// a number (they have it if they sent it). Each node runs one goroutine
// per message, so shared access goes through the lock.
var (
	mu      sync.Mutex
	known   = map[string]map[int]struct{}{}
	pending = map[string]map[int]struct{}{}
	topo    map[string][]string // our copy of the topology message
)

// Handlers can run before the init message was applied, and this node's
// data is keyed by n.ID(), so wait until init is done.
func waitForInit(n *maelstrom.Node) {
	for n.ID() == "" {
		time.Sleep(time.Millisecond)
	}
}

// setFor returns (and creates) the set for a node id. Caller holds mu.
func setFor(id string) map[int]struct{} {
	s, ok := known[id]
	if !ok {
		s = map[int]struct{}{}
		known[id] = s
	}
	return s
}

// queue adds nums to a neighbor's pending set. Caller holds no lock.
func queue(dest string, nums []int) {
	mu.Lock()
	set, ok := pending[dest]
	if !ok {
		set = map[int]struct{}{}
		pending[dest] = set
	}
	for _, m := range nums {
		set[m] = struct{}{}
	}
	mu.Unlock()
}

// sendTo fires one frame with nums. On ack, those numbers leave pending
// and enter the neighbor's known set. Sending happens outside the lock:
// RPC is async, the answer arrives on its own goroutine.
func sendTo(n *maelstrom.Node, dest string, nums []int) {
	err := n.RPC(dest,
		map[string]any{"type": "broadcast", "messages": nums},
		func(msg maelstrom.Message) error {
			mu.Lock()
			for _, m := range nums {
				delete(pending[dest], m)
				setFor(dest)[m] = struct{}{}
			}
			mu.Unlock()
			return nil
		})
	if err != nil {
		log.Print(err)
	}
}

func main() {
	n := maelstrom.NewNode()

	// Sweep: every 500ms, re-send each neighbor's unacked numbers in one
	// frame. Under a partition the frame is dropped and the numbers stay
	// pending; once the network heals, an ack clears them.
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			type job struct {
				dest string
				nums []int
			}
			jobs := []job{}
			mu.Lock()
			for dest, set := range pending {
				if len(set) == 0 {
					continue
				}
				nums := []int{}
				for m := range set {
					nums = append(nums, m)
				}
				jobs = append(jobs, job{dest, nums})
			}
			mu.Unlock()
			for _, j := range jobs {
				sendTo(n, j.dest, j.nums)
			}
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		waitForInit(n)

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// A JSON number unmarshals as float64. Clients send one "message";
		// our retry frames between nodes carry a "messages" list.
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

		// Under the lock: dedup, store, note the sender knows its numbers,
		// and pick, per neighbor, the numbers that neighbor is missing.
		mu.Lock()
		plan := map[string][]int{}
		neighbors := topo[n.ID()]
		if len(neighbors) == 0 {
			// No topology yet: be safe and use every other node.
			for _, id := range n.NodeIDs() {
				if id != n.ID() && id != msg.Src {
					neighbors = append(neighbors, id)
				}
			}
		}
		for _, m := range numbers {
			setFor(msg.Src)[m] = struct{}{}
			if _, ok := setFor(n.ID())[m]; ok {
				continue // seen before: nothing to store or forward
			}
			setFor(n.ID())[m] = struct{}{}
			for _, dest := range neighbors {
				if _, ok := setFor(dest)[m]; ok {
					continue // we know this neighbor already has m
				}
				plan[dest] = append(plan[dest], m)
			}
		}
		mu.Unlock()

		// Queue the frame for retry and fire a first send right away.
		for dest, nums := range plan {
			dest := dest
			queue(dest, nums)
			sendTo(n, dest, nums)
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		waitForInit(n)

		// Copy our own set under the lock so the reply never races the
		// broadcast handler.
		mu.Lock()
		mine := make([]int, 0, len(known[n.ID()]))
		for v := range known[n.ID()] {
			mine = append(mine, v)
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": mine})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		waitForInit(n)

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if t, ok := body["topology"].(map[string]any); ok {
			topoMap := map[string][]string{}
			for node, val := range t {
				neighbors := []string{}
				if list, ok := val.([]any); ok {
					for _, nb := range list {
						if s, ok := nb.(string); ok {
							neighbors = append(neighbors, s)
						}
					}
				}
				topoMap[node] = neighbors
			}
			mu.Lock()
			topo = topoMap
			mu.Unlock()
		}

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
