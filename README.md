# Durable TCP pub/sub broker

Topic-based publish/subscribe: publishers and subscribers talk only to the broker.
The broker persists messages and subscription state in Postgres and pushes messages
to live subscribers with at-least-once delivery semantics.

Model reference: Eugster et al., *The Many Faces of Publish/Subscribe*, ACM Computing Surveys, 2003.

## Layout

```
cmd/broker/           entrypoint
internal/protocol/    length-prefixed TCP framing + codecs
internal/server/      TCP accept loop + dispatch
internal/pubsub/      broker orchestration (live subs in memory)
internal/store/       Postgres persistence
migrations/           schema
```

## Protocol (v1)

```
[u32 big-endian length][u8 type][payload...]
```

| Type | Direction | Purpose |
|------|-----------|---------|
| PUBLISH | client → broker | durably append a message to a topic |
| PUB_OK | broker → client | confirm persistence with the message ID |
| SUBSCRIBE | client → broker | register or resume a durable consumer group |
| MSG | broker → client | deliver a topic message |
| UNSUBSCRIBE | client → broker | drop the live session mapping |
| ACK | client → broker | advance the durable subscription cursor |
| OK / ERR | broker → client | confirm a command or report an error |
| PING / PONG | either | keepalive |

`MSG` payloads use:

```
[u64 message_id][u16 topic_len][topic][body...]
```

## Delivery behavior

- `SUBSCRIBE` creates or resumes the `(topic, consumer group)` subscription. The
  broker replies with `OK`, then sends the unacknowledged backlog in message-ID order.
- A successful `PUBLISH` is committed before `PUB_OK`. The new message is also pushed
  to every live connection subscribed to that topic.
- `ACK` advances the group cursor. A reconnect can receive any message after that
  cursor again, so subscribers must be idempotent.
- Live session state is ephemeral; unsubscribe and disconnect do not delete the
  durable consumer group.

Lease recording/redelivery timing and an in-flight cap are not implemented yet.
Until those phases land, a reconnect immediately makes unacknowledged messages
eligible again, and a large or slow backlog is not bounded by broker backpressure.

## Quick start

Uses a local Postgres 18 data dir (`.pgdata` on port `5433`) so you don't need Docker
or the system Postgres password:

```bash
make db-start          # init/start local Postgres + apply migrations
go build -o broker ./cmd/broker
./broker               # listens on :9000
```

Or in one step: `make run`.

Env (see `.env.example`):

- `DATABASE_URL` — default `postgres://pubsub@localhost:5433/pubsub?sslmode=disable`
- `BROKER_ADDR` — default `:9000`

Optional: `docker compose up -d` if you prefer containerized Postgres on `:5432`
(set `DATABASE_URL` accordingly).

## Testing

Unit tests run without Postgres; database-backed tests skip unless `DATABASE_URL`
is set:

```bash
go test ./...

make db-start
DATABASE_URL='postgres://pubsub@localhost:5433/pubsub?sslmode=disable' go test ./...
```

The database-backed suite covers claim eligibility around ack cursors and leases,
plus a full TCP flow from subscribe through publish to receiving and decoding `MSG`.

## Current milestone

**Phase 3 (partial):** framing, durable publish and subscriptions, ack cursors,
backlog claims, and live `MSG` delivery on subscribe and publish.

**Not yet:** per-connection backpressure, lease persistence/redelivery, and
competing consumers (`SKIP LOCKED`).
