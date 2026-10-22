package main

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Bit layout of the 64-bit ID (MSB -> LSB):
//
//	[ 44 bits := unix milliseconds at generation ]
//	[  8 bits := node ordinal (from Maelstrom's init) ]
//	[ 10 bits := per-node sequence counter            ]
//
// The node ordinal guarantees globally disjoint ranges between nodes, and a
// moving timestamp in the highest bits means a killed-and-restarted node
// (counter, and all state, fresh to zero) can never re-visit frames it has
// already used. No coordination of any kind is required to generate an ID,
// so the service stays totally available during network partitions.
const (
	tsShift   = 18 // 44 + 8 + 10 = 62 (fits an int64; MSB stays 0)
	nodeShift = 10
	nodeMask  = 0xFF
	seqMask   = 0x3FF
)

// Per-process generator state. Every Maelstrom node runs the same binary as
// its own OS process, so each has one memory copy of these variables - that
// is why cross-process distinctness comes from the node-ordinal bits (and
// cross-restart distinctness from the millisecond watermark), not from here.
// But within ONE process the library dispatches one goroutine per message,
// so concurrent handlers share these variables; the mutex turns the
// read-modify-write of (millisecond, sequence) into a single indivisible
// step. The critical section is pure integer arithmetic - microseconds of
// lock-hold time, invisible against per-request latency budgets.
//
// (Earlier revisions: first two independent atomics with a CAS retry - which
// could commit already-minted frames at sequence-wrap boundaries - then a
// single packed-atomic CAS loop. A mutex is the simpler, equally correct
// fit at this traffic level: the shared state is trivially tiny, and plain
// sequential code under one lock expresses the invariants directly.)
var (
	mu     sync.Mutex // guards lastMS and seq
	lastMS uint64     // highest unix-millisecond minted so far
	seq    uint64     // sequence consumed within lastMS (1024 IDs per ms)
)

func nextID(nodeID uint64) uint64 {
	mu.Lock()
	defer mu.Unlock()

	// Lazy one-time seed, on the first call in this process.
	if lastMS == 0 {
		lastMS = uint64(time.Now().UnixMilli())
	}

	now := uint64(time.Now().UnixMilli())
	if now < lastMS {
		// The clock stepped backwards (nemesis/ntp): freeze it at the
		// highest millisecond minted so far so no old frame can repeat.
		now = lastMS
	}

	if now > lastMS {
		// Real time has moved on: restart the sequence in the new frame.
		seq = 0
		lastMS = now
	}

	seq++
	if seq > seqMask {
		// 1024 IDs minted inside one millisecond: roll the sequence over
		// and advance the embedded millisecond instead.
		seq = 0
		lastMS++
	}

	return lastMS<<tsShift | nodeID<<nodeShift | seq
}

// nodeOrdinal parses "n3" -> 3 from the init message received from Maelstrom.
// Maelstrom guarantees these suffixes are unique per node. It is resolved
// once, right after init, and only falls back to hashing the node ID if the
// name ever deviates from the n<integer> convention.
func nodeOrdinal(n *maelstrom.Node) uint64 {
	if v, err := strconv.ParseUint(strings.TrimPrefix(n.ID(), "n"), 10, 8); err == nil {
		return v
	}
	// Defensive fallback: hash the full node ID (unreachable in practice,
	// Maelstrom always names nodes n1, n2, ...).
	h := fnv.New64a()
	_, _ = h.Write([]byte(n.ID()))
	return h.Sum64() & nodeMask
}

// waitForNodeID resolves and caches this node's ordinal. Handler goroutines
// may race the init handler, so it waits (bounded by the test lifetime)
// until init has been processed and n.ID() is populated.
func waitForNodeID(n *maelstrom.Node) uint64 {
	for n.ID() == "" {
		time.Sleep(time.Millisecond)
	}
	return nodeOrdinal(n)
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Maelstrom's Reply round-trips the body through map[string]any,
		// where JSON numbers become float64 (53 bits of mantissa). A 64-bit
		// integer ID cannot survive that trip: adjacent values collapse to
		// the same float and the checker sees duplicates. Serializing the
		// ID as a decimal string keeps all 64 bits intact - and the challenge
		// explicitly allows IDs of any type.
		body["type"] = "generate_ok"
		body["id"] = strconv.FormatUint(nextID(waitForNodeID(n)), 10)

		return n.Reply(msg, body)
	})

	// Seed the generator as soon as Maelstrom tells this node who it is,
	// before the first request can ever be answered.
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
