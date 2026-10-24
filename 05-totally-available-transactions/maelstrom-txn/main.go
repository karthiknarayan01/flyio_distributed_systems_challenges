package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node

	// keysLock guards the locks map itself; the map holds one lock per
	// key, so a read and a write of the same key never overlap.
	keysLock sync.Mutex
	keyLocks = map[int]*sync.Mutex{}
)

// values holds one stored value per key, safe for concurrent use:
// operations on different keys touch the map at the same time.
var values sync.Map

func main() {
	node = maelstrom.NewNode()

	node.Handle("txn", handleTxn)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}

// handleTxn runs a transaction's operations in order. Each operation
// locks only its own key, does its work, and releases the lock at once,
// so other transactions are never blocked longer than one operation.
func handleTxn(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	out := make([]any, 0, len(body["txn"].([]any)))
	for _, rawOp := range body["txn"].([]any) {
		op := rawOp.([]any)
		name := op[0].(string)
		key := int(op[1].(float64))

		switch name {
		case "r":
			out = append(out, []any{name, key, readLocal(key)})
		case "w":
			value := int(op[2].(float64))
			writeLocal(key, value)
			out = append(out, []any{name, key, value})
		}
	}

	return node.Reply(msg, map[string]any{"type": "txn_ok", "txn": out})
}

// readLocal answers a key's stored value, or nil when the key was never
// written.
func readLocal(key int) any {
	lock := lockFor(key)
	lock.Lock()
	value, ok := values.Load(key)
	lock.Unlock()
	if !ok {
		return nil
	}
	return value
}

// lockFor returns a key's lock, created on first use.
func lockFor(key int) *sync.Mutex {
	keysLock.Lock()
	lock, ok := keyLocks[key]
	if !ok {
		lock = &sync.Mutex{}
		keyLocks[key] = lock
	}
	keysLock.Unlock()
	return lock
}

// writeLocal stores a value under a key.
func writeLocal(key, value int) {
	lock := lockFor(key)
	lock.Lock()
	values.Store(key, value)
	lock.Unlock()
}
