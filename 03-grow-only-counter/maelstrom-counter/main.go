// Command maelstrom-counter implements the grow-only counter.
//
// The approach: one total lives under a single key in the sequential
// key/value store Maelstrom provides, so a count can never fork. An
// add reads the total and compares-and-swaps it up by its delta,
// re-reading and retrying whenever another node moved the key first —
// meaning an add acks only once it is safely in the store, which a
// later partition cannot undo. A read first pushes the store's view
// forward with one unique write to a scratch key (store.go), so the
// read after it reflects the counter's total exactly — that is what
// keeps the test's final reads correct.
//
// Both moves are the store talk in store.go; this file is the two
// handlers.
package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node
	kv   *maelstrom.KV
)

func main() {
	node = maelstrom.NewNode()
	kv = maelstrom.NewSeqKV(node)

	node.Handle("add", handleAdd)
	node.Handle("read", handleRead)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}

// handleAdd acks only after the add sits safely in the shared store:
// an acked add can then never be lost, even by a later partition.
func handleAdd(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if err := increment(int(body["delta"].(float64))); err != nil {
		return err
	}

	return node.Reply(msg, map[string]any{"type": "add_ok"})
}

func handleRead(msg maelstrom.Message) error {
	total, err := readTotal()
	if err != nil {
		return err
	}

	return node.Reply(msg, map[string]any{"type": "read_ok", "value": total})
}
