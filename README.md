# Fly.io Distributed Systems Challenges

Solutions to the [Fly.io Distributed Systems Challenges](https://fly.io/dist-sys/) ("Gossip Glomers"), tested against the [Maelstrom](https://github.com/jepsen-io/maelstrom) cluster simulator.

## Challenges

| # | Challenge | Folder | Guide |
|---|-----------|--------|-------|
| 0 | Echo | [`00-echo`](./00-echo) | [Echo](https://fly.io/dist-sys/challenges/echo/) |
| 1 | Unique ID Generation | [`01-unique-id-generation`](./01-unique-id-generation) | [Unique ID Generation](https://fly.io/dist-sys/challenges/unique-id-generation/) |
| 2 | Broadcast | [`02-broadcast`](./02-broadcast) | [Reliable Broadcast](https://fly.io/dist-sys/challenges/broadcast/) |
| 3 | Grow-Only Counter | [`03-grow-only-counter`](./03-grow-only-counter) | [Grow-Only Counter](https://fly.io/dist-sys/challenges/grow-only-counter/) |
| 4 | Kafka-Style Log | [`04-kafka-style-log`](./04-kafka-style-log) | [Kafka-Style Log](https://fly.io/dist-sys/challenges/kafka-log/) |
| 5 | Totally-Available Transactions | [`05-totally-available-transactions`](./05-totally-available-transactions) | [Totally-Available Transactions](https://fly.io/dist-sys/challenges/transactions/) |

## About

Each challenge builds on the previous one and focuses on a core distributed systems concept:

- **Echo** — a warm-up: basic node setup and message handling with Maelstrom.
- **Unique ID Generation** — generating globally unique IDs across nodes.
- **Broadcast** — reliable, efficient gossip-based message propagation.
- **Grow-Only Counter** — a distributed counter with conflict-free convergence.
- **Kafka-Style Log** — a replicated append-only log service.
- **Totally-Available Transactions** — a transactional key-value store that stays available during partitions.

## Running

Solutions are validated with the Maelstrom test runner:

```bash
# example: run the echo challenge
./maelstrom test -w echo --bin ./00-echo/echo --node-count 1 --time-limit 10
```
