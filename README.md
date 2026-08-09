# Durable TCP pub/sub broker

Topic-based publish/subscribe: publishers and subscribers talk only to the broker.
The broker persists messages and subscription state in Postgres (at-least-once delivery comes next).

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
| PUBLISH / PUB_OK | client → broker → client | durable publish |
| SUBSCRIBE / OK | client → broker → client | register consumer group (persistent) |
| UNSUBSCRIBE / OK | client → broker → client | drop live session mapping |
| ACK / OK | client → broker → client | advance cursor |
| PING / PONG | either | keepalive |

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

## Current milestone

**Phase 2 (scaffolded):** framing, publish persistence, subscribe upsert, ack cursor, TCP server.

**Not yet:** push delivery of `MSG` frames, leases/redelivery, competing consumers (`SKIP LOCKED`).
