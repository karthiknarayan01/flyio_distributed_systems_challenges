// The in-memory log model: one logState per named log, touched only by
// its owning node, so the per-log lock only ever serializes concurrent
// handlers of that one node. Local reads and writes and the committed
// marker live here; copying changes out to the store is the flusher's
// job.

package main

import "sync"

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

// logKey maps a user log name to its store key.
func logKey(name string) string {
	return "log:" + name
}
