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

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Reply round-trips bodies through map[string]any, where JSON
		// numbers become float64 (53-bit mantissa); a 64-bit integer ID
		// would lose bits. Strings survive intact.
		body["type"] = "generate_ok"
		body["id"] = strconv.FormatUint(nextID(waitForNodeID(n)), 10)

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}

// waitForNodeID waits until the init message has been processed: handler
// goroutines may race the init handler, so n.ID() may not be populated yet.
func waitForNodeID(n *maelstrom.Node) uint64 {
	for n.ID() == "" {
		time.Sleep(time.Millisecond)
	}
	return nodeOrdinal(n)
}

// nodeOrdinal parses "n3" -> 3. Falls back to hashing the node ID if it
// deviates from the n<integer> convention.
func nodeOrdinal(n *maelstrom.Node) uint64 {
	if v, err := strconv.ParseUint(strings.TrimPrefix(n.ID(), "n"), 10, 8); err == nil {
		return v
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(n.ID()))
	return h.Sum64() & nodeMask
}

// Bit layout of the 64-bit ID (MSB -> LSB):
//
//	[ 44 bits := unix milliseconds at generation ]
//	[  8 bits := node ordinal (from Maelstrom's init) ]
//	[ 10 bits := per-node sequence counter            ]
//
// The node ordinal keeps node ranges disjoint, the moving timestamp covers
// process restarts, and generation needs no coordination.
const (
	tsShift   = 18 // 44 + 8 + 10 = 62 (fits an int64; MSB stays 0)
	nodeShift = 10
	nodeMask  = 0xFF
	seqMask   = 0x3FF
)

// Per-process generator state, guarded so concurrent handler goroutines
// serialize on the (millisecond, sequence) update. Cross-process
// distinctness comes from the node ordinal bits.
var (
	mu     sync.Mutex // guards lastMS and seq
	lastMS uint64     // highest unix-millisecond minted so far
	seq    uint64     // sequence consumed within lastMS (1024 IDs per ms)
)

func nextID(nodeID uint64) uint64 {
	mu.Lock()
	defer mu.Unlock()

	// Lazy one-time seed.
	if lastMS == 0 {
		lastMS = uint64(time.Now().UnixMilli())
	}

	now := uint64(time.Now().UnixMilli())
	if now < lastMS {
		// Clock stepped backwards: keep the highest minted millisecond.
		now = lastMS
	}

	if now > lastMS {
		// New millisecond: restart the sequence.
		seq = 0
		lastMS = now
	}

	seq++
	if seq > seqMask {
		// 1024 IDs minted in this millisecond: roll over and advance it.
		seq = 0
		lastMS++
	}

	return lastMS<<tsShift | nodeID<<nodeShift | seq
}
