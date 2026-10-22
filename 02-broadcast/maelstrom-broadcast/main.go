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
// A neighbor's set grows two ways: on an ack, and when they send us a
// number (they have it if they sent it). Each node runs one goroutine per
// message, so shared access goes through the lock.
var (
	mu    sync.Mutex
	known = map[string]map[int]struct{}{}
	topo  map[string][]string // our copy of the topology message
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

func main() {
	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		waitForInit(n)

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// A JSON number unmarshals as float64.
		m := int(body["message"].(float64))

		// Under the lock: dedup, store, and pick who is missing this number.
		mu.Lock()
		mine := setFor(n.ID())
		if _, ok := mine[m]; ok {
			mu.Unlock()
			return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
		}
		mine[m] = struct{}{}
		setFor(msg.Src)[m] = struct{}{} // the sender just proved it has m

		targets := []string{}
		neighbors := topo[n.ID()]
		if len(neighbors) == 0 {
			// No topology yet: be safe and use every other node.
			for _, id := range n.NodeIDs() {
				if id != n.ID() && id != msg.Src {
					neighbors = append(neighbors, id)
				}
			}
		}
		for _, dest := range neighbors {
			if _, ok := setFor(dest)[m]; ok {
				continue // we know this neighbor already has m
			}
			targets = append(targets, dest)
		}
		mu.Unlock()

		// Send outside the lock: RPC is async, the answer arrives on its
		// own goroutine. An ack adds the number to the neighbor's set.
		for _, dest := range targets {
			dest := dest
			err := n.RPC(dest,
				map[string]any{"type": "broadcast", "message": m},
				func(msg maelstrom.Message) error {
					mu.Lock()
					setFor(dest)[m] = struct{}{}
					mu.Unlock()
					return nil
				})
			if err != nil {
				log.Print(err)
			}
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
