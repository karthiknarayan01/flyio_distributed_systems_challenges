// Who owns which log, and how to reach them: the deterministic hash
// pick that every node pays the same, the init wait that handlers
// depend on before ownership is known, and the synchronous relay call
// that rides out timeouts with a retry.

package main

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"sort"
	"time"
)

// waitForInit: ownership picks and relaying both need the node ids and
// our own id, which arrive in the init message.
func waitForInit() {
	for node.ID() == "" {
		time.Sleep(time.Millisecond)
	}
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
