// Our own network layout. Instead of the topology Maelstrom suggests,
// every node computes the same star on its own: the lowest id relays
// for everyone, everyone else sends only to the hub — which is what
// handleBroadcast consults to pick recipients. handleTopology lives
// here too, since a suggested topology changes nothing for us.

package main

import (
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func handleTopology(msg maelstrom.Message) error {
	// Maelstrom still sends its suggested topology; we only ack it and
	// keep our own star neighbor list.
	return node.Reply(msg, map[string]any{"type": "topology_ok"})
}

// ourNeighbors returns who we forward numbers to. Our network is a star:
// the hub sends to everyone, each other node only sends to the hub.
// Every node knows this list by itself, so we ignore the topology
// Maelstrom suggests.
func ourNeighbors() []string {
	if node.ID() == hubOf() {
		others := []string{}
		for _, id := range node.NodeIDs() {
			if id != node.ID() {
				others = append(others, id)
			}
		}
		return others
	}
	return []string{hubOf()}
}

// hubOf returns the node that relays every broadcast in our star
// topology. Every node picks the same one: the lowest node id.
func hubOf() string {
	hub := ""
	for _, id := range node.NodeIDs() {
		if hub == "" || id < hub {
			hub = id
		}
	}
	return hub
}
