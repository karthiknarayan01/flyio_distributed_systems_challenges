package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	node *maelstrom.Node
	kv   *maelstrom.KV
)

// rpcTimeout bounds one store call; the caller then reads again.
const rpcTimeout = 500 * time.Millisecond

// retryPause waits after a failed attempt so retries do not hammer
// the store.
const retryPause = 10 * time.Millisecond

// logKey maps a user log name to its store key. The whole log lives
// under one key as {"messages": [...], "committed": n}, and a
// compare-and-swap on that single value makes every update atomic:
// offsets stay monotonic and no acked send can be lost.
func logKey(log string) string {
	return "log:" + log
}

// logView is one snapshot of a log.
type logView struct {
	// raw is the value exactly as the store gave it. It serves as the
	// from side of a compare-and-swap, so a concurrent change on
	// another node makes our swap fail instead of landing on top of it.
	// nil means the key does not exist yet.
	raw       any
	messages  []int
	committed int
}

// readLog returns the current log, retrying through store calls that
// fail for timing reasons. A missing key is an empty log: raw is nil
// and nothing is committed.
func readLog(key string) *logView {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		value, err := kv.Read(ctx, key)
		cancel()

		switch {
		case maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist:
			return &logView{committed: -1}
		case err != nil:
			time.Sleep(retryPause)
		default:
			return newView(value)
		}
	}
}

// newView parses one stored log value. The store only ever holds values
// this program wrote, so the shape is stable.
func newView(value any) *logView {
	stored := value.(map[string]any)
	messages := make([]int, 0, len(stored["messages"].([]any)))
	for _, raw := range stored["messages"].([]any) {
		messages = append(messages, int(raw.(float64)))
	}
	return &logView{
		raw:       value,
		messages:  messages,
		committed: int(stored["committed"].(float64)),
	}
}

// casLog stores one snapshot under a key if the snapshot the view came
// from is still the stored one. The loop callers run handles all the
// losses: a failed precondition means another node changed the log, and
// a timed-out swap that did land is visible in the next read, so
// starting over from a fresh view is always safe.
func casLog(key string, view, next *logView) error {
	// The fields go through plain JSON objects; the store compares
	// values, and equal numbers compare equal whichever side of a round
	// trip they came from.
	payload := map[string]any{
		"messages":  next.messages,
		"committed": next.committed,
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	err := kv.CompareAndSwap(ctx, key, view.raw, payload, view.raw == nil)
	cancel()
	return err
}

// appendLog adds one message through the compare-and-swap loop and
// answers with its offset. Offsets are dense from zero, so a fresh
// message's offset is the last index, which only grows.
func appendLog(key string, value int) (offset int) {
	for {
		view := readLog(key)
		messages := append(append([]int{}, view.messages...), value)
		next := &logView{messages: messages, committed: view.committed}
		if err := casLog(key, view, next); err == nil {
			return len(messages) - 1
		}
		time.Sleep(retryPause)
	}
}

// commitLog moves a log's committed marker forward, by writing it only
// when it grows: a client may commit an offset we already hold.
func commitLog(key string, commitTo int) {
	for {
		view := readLog(key)
		if commitTo <= view.committed {
			return
		}
		next := &logView{messages: view.messages, committed: commitTo}
		if err := casLog(key, view, next); err == nil {
			return
		}
		time.Sleep(retryPause)
	}
}

// handleSend acks only after the send sits safely in the shared store.
func handleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offset := appendLog(logKey(body["key"].(string)), int(body["msg"].(float64)))

	return node.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
}

// handlePoll answers, per requested key, the [offset, msg] pairs from
// the given offset onward. A key with nothing at or after the offset is
// left out of the answer.
func handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	out := map[string][][2]int{}
	for name, rawOffset := range body["offsets"].(map[string]any) {
		from := int(rawOffset.(float64))
		if from < 0 {
			from = 0
		}

		view := readLog(logKey(name))
		if from >= len(view.messages) {
			continue
		}

		pairs := make([][2]int, 0, len(view.messages)-from)
		for i := from; i < len(view.messages); i++ {
			pairs = append(pairs, [2]int{i, view.messages[i]})
		}
		out[name] = pairs
	}

	return node.Reply(msg, map[string]any{"type": "poll_ok", "msgs": out})
}

func handleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	for name, rawOffset := range body["offsets"].(map[string]any) {
		commitLog(logKey(name), int(rawOffset.(float64)))
	}

	return node.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
}

// handleListCommittedOffsets answers the committed offset for the
// requested keys, leaving out keys we never committed.
func handleListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	out := map[string]int{}
	for _, raw := range body["keys"].([]any) {
		name := raw.(string)
		view := readLog(logKey(name))
		if view.committed != -1 {
			out[name] = view.committed
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

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
