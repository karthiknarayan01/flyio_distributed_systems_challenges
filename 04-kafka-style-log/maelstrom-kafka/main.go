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
	lock sync.Mutex

	// logs maps a key to its log: the messages in offset order, and the
	// offset processed up to by consumers. messages[i] lives at offset i,
	// so offsets are dense and never move; nothing is committed when
	// committed is -1.
	logs = map[string]*logState{}
)

type logState struct {
	messages  []int
	committed int
}

// logFor returns (and creates) the state of a key. Caller holds lock.
func logFor(key string) *logState {
	state, ok := logs[key]
	if !ok {
		state = &logState{committed: -1}
		logs[key] = state
	}
	return state
}

// handleSend appends one message and answers with its offset. The
// offset of a fresh message is the last index, which only grows.
func handleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	value := int(body["msg"].(float64))

	lock.Lock()
	state := logFor(body["key"].(string))
	state.messages = append(state.messages, value)
	offset := len(state.messages) - 1
	lock.Unlock()

	return node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

// handlePoll returns, per requested key, the messages from the given
// offset onward as [offset, msg] pairs. A key we have nothing for is
// simply left out of the answer.
func handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets := body["offsets"].(map[string]any)

	out := map[string][][2]int{}
	lock.Lock()
	for key, raw := range offsets {
		from := int(raw.(float64))
		if from < 0 {
			from = 0
		}
		state, ok := logs[key]
		if !ok || from >= len(state.messages) {
			continue
		}
		pairs := make([][2]int, 0, len(state.messages)-from)
		for i := from; i < len(state.messages); i++ {
			pairs = append(pairs, [2]int{i, state.messages[i]})
		}
		out[key] = pairs
	}
	lock.Unlock()

	return node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": out})
}

// handleCommitOffsets records that a log was processed up to a given
// offset. The marker only moves forward: a client may commit an older
// offset than we already have.
func handleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets := body["offsets"].(map[string]any)

	lock.Lock()
	for key, raw := range offsets {
		commitTo := int(raw.(float64))
		state := logFor(key)
		if commitTo > state.committed {
			state.committed = commitTo
		}
	}
	lock.Unlock()

	return node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

// handleListCommittedOffsets answers the committed offset for the
// requested keys, leaving out keys we never committed.
func handleListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	keys := body["keys"].([]any)

	out := map[string]int{}
	lock.Lock()
	for _, raw := range keys {
		state, ok := logs[raw.(string)]
		if ok && state.committed != -1 {
			out[raw.(string)] = state.committed
		}
	}
	lock.Unlock()

	return node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": out,
	})
}

func main() {
	node = maelstrom.NewNode()

	node.Handle("send", handleSend)
	node.Handle("poll", handlePoll)
	node.Handle("commit_offsets", handleCommitOffsets)
	node.Handle("list_committed_offsets", handleListCommittedOffsets)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
