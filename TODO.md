# TODO — next features

Ordered for the durable broker path. Check items off as they land.

## Phase 3 — Push delivery

- [x] Claim undelivered messages for an active subscription (after `last_acked_id`, not leased)
- [x] Write `MSG` frames to the live TCP connection on subscribe and on new publish
- [ ] Cap in-flight messages per connection (backpressure)
- [x] Integration test: subscribe → publish → receive `MSG`

## Phase 4 — Leases & redelivery

- [ ] Insert/update `deliveries` rows when sending `MSG` (`leased_until`, `attempt`)
- [ ] On `ACK`: clear delivery + advance `subscription_offsets.last_acked_id`
- [ ] Background reaper: expire leases → allow redelivery
- [ ] On disconnect: leave durable state; let leases expire (or release immediately)
- [ ] Test: kill subscriber mid-flight → message redelivered after lease expiry

## Phase 5 — Competing consumers

- [ ] Multiple connections sharing the same `(topic, group)`
- [ ] Claim with `FOR UPDATE SKIP LOCKED` so each message goes to one worker
- [ ] Test: two workers, N messages, no duplicate successful acks under healthy run

## Phase 6 — Hardening

- [ ] Message retention / truncate once all subscriptions have acked past an id (or TTL)
- [ ] Dead-letter after N delivery attempts
- [ ] Heartbeat idle timeout (close dead conns)
- [ ] Basic metrics (publish rate, ack rate, in-flight, redeliveries)
- [ ] Small CLI or example client under `cmd/` for publish/subscribe demos

## Later / stretch

- [ ] Explicit `CREATE_TOPIC` / `DELETE_SUBSCRIPTION` admin commands
- [ ] Pull mode (`PULL` / batch fetch) alongside push
- [ ] Multi-broker workers sharing one Postgres (no gossip yet)
- [ ] Optional WebSocket gateway for browser demos
