package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Handlers run one goroutine per message, so the list and the topology map
// need the lock. Cross-process duplicates are not a concern in this part:
// a single node just answers its own clients, so nothing is forwarded yet.
var (
	mu       sync.Mutex
	messages []int
	topology map[string][]string
)

// waitForInit waits until the init message has been applied. It arrives
// first on stdin, but handler goroutines can race it, so we wait until
// n.ID() is set. This keeps our replies stamped with the right src.
func waitForInit(n *maelstrom.Node) {
	for n.ID() == "" {
		time.Sleep(time.Millisecond)
	}
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
		mu.Lock()
		messages = append(messages, int(body["message"].(float64)))
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		waitForInit(n)

		// Copy under the lock so the reply can never race an append.
		mu.Lock()
		snapshot := make([]int, len(messages))
		copy(snapshot, messages)
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": snapshot})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		waitForInit(n)

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Save the neighbor map. We only return topology_ok for now.
		if t, ok := body["topology"].(map[string]any); ok {
			topo := map[string][]string{}
			for node, val := range t {
				neighbors := []string{}
				if list, ok := val.([]any); ok {
					for _, nb := range list {
						if s, ok := nb.(string); ok {
							neighbors = append(neighbors, s)
						}
					}
				}
				topo[node] = neighbors
			}

			mu.Lock()
			topology = topo
			mu.Unlock()
		}

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
