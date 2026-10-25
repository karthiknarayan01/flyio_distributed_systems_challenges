// Persistence: a background loop copies each changed log we own into
// the linear KV store, a whole log per write, since exactly one node —
// its owner — ever writes its key. A log stays marked dirty until the
// store accepts its write.

package main

import (
	"context"
	"time"
)

// flushInterval is how often a log owner copies its changed logs into
// the store.
const flushInterval = 250 * time.Millisecond

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
