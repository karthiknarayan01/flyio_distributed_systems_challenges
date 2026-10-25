// The four message handlers of the kafka workload. Each figures out
// which node owns the named logs, answers logs we own directly, and
// relays the rest to their owner, passing that owner's reply back to
// the client.

package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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
