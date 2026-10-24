package main

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node
	kv   *maelstrom.KV
)

// One store call gets this long to answer, then the caller tries again.
const rpcTimeout = 500 * time.Millisecond

// retryPause waits after a failed attempt so retries do not hammer the
// peer.
const retryPause = 10 * time.Millisecond

// flushInterval is how often a log owner copies its changed keys into
// the store.
const flushInterval = 250 * time.Millisecond

// logKey maps a user log name to its store key.
func logKey(name string) string {
	return "log:" + name
}

// ownerOf returns the node that owns a log: the same pick on every node,
// derived from a hash of the name, so no coordination is needed.
func ownerOf(name string) string {
	ids := append([]string{}, node.NodeIDs()...)
	sort.Strings(ids)
	return ids[fnv32(name)%uint32(len(ids))]
}

func fnv32(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32()
}

// waitForInit: ownership picks and relaying both need the node ids and
// our own id, which arrive in the init message.
func waitForInit() {
	for node.ID() == "" {
		time.Sleep(time.Millisecond)
	}
}

// logState is one log, kept in the owner's memory. Only the owning node
// touches it, which is what keeps offsets monotonic without any
// compare-and-swap on the log data.
type logState struct {
	lock      sync.Mutex
	messages  []int
	committed int
	// dirty marks logs changed since the last flush to the store.
	dirty bool
}

// logs maps a log name to its state. Entries are never removed, so
// pointers into the map stay valid without the map lock.
var (
	logsLock sync.Mutex
	logs     = map[string]*logState{}
)

// stateFor returns (and creates) the state of a log.
func stateFor(name string) *logState {
	logsLock.Lock()
	state, ok := logs[name]
	if !ok {
		state = &logState{committed: -1}
		logs[name] = state
	}
	logsLock.Unlock()
	return state
}

// appendLocal appends a message to our own log and answers with its
// offset. Offsets are dense from zero, so a fresh message's offset is
// the last index, which only grows.
func appendLocal(name string, value int) int {
	state := stateFor(name)
	state.lock.Lock()
	state.messages = append(state.messages, value)
	state.dirty = true
	offset := len(state.messages) - 1
	state.lock.Unlock()
	return offset
}

// pollLocal answers one group of poll offsets from our own state,
// leaving out keys with nothing at or after the requested offset.
func pollLocal(group map[string]int) map[string][][2]int {
	out := map[string][][2]int{}
	for name, rawFrom := range group {
		from := rawFrom
		if from < 0 {
			from = 0
		}

		state := stateFor(name)
		state.lock.Lock()
		if from < len(state.messages) {
			pairs := make([][2]int, 0, len(state.messages)-from)
			for i := from; i < len(state.messages); i++ {
				pairs = append(pairs, [2]int{i, state.messages[i]})
			}
			out[name] = pairs
		}
		state.lock.Unlock()
	}
	return out
}

// commitLocal moves a log's committed marker forward, but only ever
// upward: a client may commit an offset we already hold.
func commitLocal(name string, commitTo int) {
	state := stateFor(name)
	state.lock.Lock()
	if commitTo > state.committed {
		state.committed = commitTo
		state.dirty = true
	}
	state.lock.Unlock()
}

// committedLocal answers which of the named logs we own are committed,
// leaving out keys we never committed.
func committedLocal(names []string) map[string]int {
	out := map[string]int{}
	for _, name := range names {
		state := stateFor(name)
		state.lock.Lock()
		if state.committed != -1 {
			out[name] = state.committed
		}
		state.lock.Unlock()
	}
	return out
}

// runFlusher copies every changed log we own into the store, all at
// once every 250ms, with one plain Write per log. A Write is safe
// because each log belongs to exactly one node, so no two nodes ever
// write the same key and no compare-and-swap is needed.
func runFlusher() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for range ticker.C {
		dirty := []string{}
		logsLock.Lock()
		for name, state := range logs {
			if ownerOf(name) != node.ID() {
				continue
			}
			state.lock.Lock()
			if state.dirty {
				dirty = append(dirty, name)
			}
			state.lock.Unlock()
		}
		logsLock.Unlock()

		for _, name := range dirty {
			state := logs[name]
			state.lock.Lock()
			payload := map[string]any{
				"messages":  state.messages,
				"committed": state.committed,
			}
			state.dirty = false
			state.lock.Unlock()

			for {
				ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
				err := kv.Write(ctx, logKey(name), payload)
				cancel()
				if err == nil {
					break
				}
				// Waits for the store to answer, and keeps the log
				// marked dirty so the next round carries it again.
				time.Sleep(retryPause)
			}
		}
	}
}

// askOwner sends one request body to the owning node and answers with
// its reply body. The loop rides out calls that time out for timing
// reasons.
func askOwner(owner string, body any) (map[string]any, error) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		resp, err := node.SyncRPC(ctx, owner, body)
		cancel()
		if err != nil {
			time.Sleep(retryPause)
			continue
		}

		var answer map[string]any
		if err := json.Unmarshal(resp.Body, &answer); err != nil {
			return nil, err
		}
		return answer, nil
	}
}

// parsePoll adds an owner's poll answer into out as our pair shape.
func parsePoll(raw map[string]any, out map[string][][2]int) {
	for name, value := range raw["msgs"].(map[string]any) {
		pairs := make([][2]int, 0, len(value.([]any)))
		for _, item := range value.([]any) {
			vals := item.([]any)
			pairs = append(pairs, [2]int{int(vals[0].(float64)), int(vals[1].(float64))})
		}
		out[name] = pairs
	}
}

func handleSend(msg maelstrom.Message) error {
	waitForInit()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	name := body["key"].(string)

	// Logs belong to one node; requests for the others are relayed.
	owner := ownerOf(name)
	if owner != node.ID() {
		relayed, err := askOwner(owner, map[string]any{
			"type": "send",
			"key":  name,
			"msg":  body["msg"],
		})
		if err != nil {
			return err
		}
		return node.Reply(msg, relayed)
	}

	return node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": appendLocal(name, int(body["msg"].(float64))),
	})
}

func handlePoll(msg maelstrom.Message) error {
	waitForInit()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// One poll may name logs owned by different nodes, so the offsets
	// are grouped per owner and gathered one group at a time.
	groups := map[string]map[string]int{}
	for name, rawOffset := range body["offsets"].(map[string]any) {
		owner := ownerOf(name)
		if groups[owner] == nil {
			groups[owner] = map[string]int{}
		}
		groups[owner][name] = int(rawOffset.(float64))
	}

	out := map[string][][2]int{}
	for owner, group := range groups {
		if owner == node.ID() {
			for name, pairs := range pollLocal(group) {
				out[name] = pairs
			}
			continue
		}
		relayed, err := askOwner(owner, map[string]any{"type": "poll", "offsets": group})
		if err != nil {
			return err
		}
		parsePoll(relayed, out)
	}

	return node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": out})
}

func handleCommitOffsets(msg maelstrom.Message) error {
	waitForInit()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	groups := map[string]map[string]int{}
	for name, rawOffset := range body["offsets"].(map[string]any) {
		owner := ownerOf(name)
		if groups[owner] == nil {
			groups[owner] = map[string]int{}
		}
		groups[owner][name] = int(rawOffset.(float64))
	}

	for owner, group := range groups {
		if owner == node.ID() {
			for name, commitTo := range group {
				commitLocal(name, commitTo)
			}
			continue
		}
		if _, err := askOwner(owner, map[string]any{"type": "commit_offsets", "offsets": group}); err != nil {
			return err
		}
	}

	return node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

func handleListCommittedOffsets(msg maelstrom.Message) error {
	waitForInit()

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	groups := map[string][]string{}
	for _, raw := range body["keys"].([]any) {
		name := raw.(string)
		owner := ownerOf(name)
		groups[owner] = append(groups[owner], name)
	}

	out := map[string]int{}
	for owner, names := range groups {
		if owner == node.ID() {
			for name, offset := range committedLocal(names) {
				out[name] = offset
			}
			continue
		}
		relayed, err := askOwner(owner, map[string]any{"type": "list_committed_offsets", "keys": names})
		if err != nil {
			return err
		}
		for name, raw := range relayed["offsets"].(map[string]any) {
			out[name] = int(raw.(float64))
		}
	}

	return node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": out,
	})
}

func main() {
	node = maelstrom.NewNode()
	kv = maelstrom.NewLinKV(node)

	node.Handle("send", handleSend)
	node.Handle("poll", handlePoll)
	node.Handle("commit_offsets", handleCommitOffsets)
	node.Handle("list_committed_offsets", handleListCommittedOffsets)

	go runFlusher()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
