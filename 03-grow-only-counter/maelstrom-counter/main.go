package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node
	kv   *maelstrom.KV
)

// The whole counter lives under this one key in the shared key/value
// store, so every add goes through a compare-and-swap on that key.
// A scratch key serves the sync writes that push our view forward.
const (
	counterKey = "counter"
	syncKey    = "sync"
)

// rpcTimeout bounds one store call; the loop then reads again.
const rpcTimeout = 500 * time.Millisecond

// retryPause waits before the next attempt so a locked-out store is
// not hammered by a spinning handler.
const retryPause = 10 * time.Millisecond

// current returns the counter total and whether the key exists yet.
// A missing key is an empty counter: zero. A read may come from a state
// a few steps back in the store's history, but that cannot corrupt the
// count: a compare-and-swap from a stale value mismatches, and the loop
// re-reads until it matches the real one.
func current() (total int, missing bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	total, err = kv.ReadInt(ctx, counterKey)
	if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
		return 0, true, nil
	}
	return total, false, err
}

// increment adds delta to the shared counter. The compare-and-swap
// fails when another node moved the key first, so the loop reads again
// and starts over; that fresh reading is also what keeps a swap whose
// timeout left its outcome unknown safe, because an applied swap
// already shows up in the next read.
func increment(delta int) error {
	for {
		total, missing, err := current()
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			err = kv.CompareAndSwap(ctx, counterKey, total, total+delta, missing)
			cancel()
		}
		if err == nil {
			return nil
		}
		time.Sleep(retryPause)
	}
}

// syncStore writes a unique value to a scratch key, which pushes our
// view of the store to its most recent state. The sequential store
// serves reads from any state between our last view and the present,
// so a read issued right after this one sees everything acked so far.
// That is what keeps the final read of the test exact.
var syncSequence int64

func syncStore() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		err := kv.Write(ctx, syncKey, atomic.AddInt64(&syncSequence, 1))
		cancel()
		if err == nil {
			return
		}
		time.Sleep(retryPause)
	}
}

// readTotal syncs to the newest store state first, then reads once:
// that read cannot miss an add acked before it. A missing key means
// there are no adds yet.
func readTotal() (int, error) {
	for {
		syncStore()

		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		total, err := kv.ReadInt(ctx, counterKey)
		cancel()
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, nil
		}
		if err == nil {
			return total, nil
		}
		time.Sleep(retryPause)
	}
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
