-- Durable topic-based pub/sub schema.
-- Topics + messages form the append-only log; subscriptions hold consumer-group state.

CREATE TABLE topics (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE messages (
    id         BIGSERIAL PRIMARY KEY,
    topic_id   BIGINT NOT NULL REFERENCES topics (id),
    payload    BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX messages_topic_id_id_idx ON messages (topic_id, id);

CREATE TABLE subscriptions (
    id         BIGSERIAL PRIMARY KEY,
    topic_id   BIGINT NOT NULL REFERENCES topics (id),
    name       TEXT NOT NULL, -- consumer group
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_id, name)
);

CREATE TABLE subscription_offsets (
    subscription_id BIGINT PRIMARY KEY REFERENCES subscriptions (id) ON DELETE CASCADE,
    last_acked_id   BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE deliveries (
    subscription_id BIGINT NOT NULL REFERENCES subscriptions (id) ON DELETE CASCADE,
    message_id      BIGINT NOT NULL REFERENCES messages (id),
    attempt         INT NOT NULL DEFAULT 1,
    leased_until    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (subscription_id, message_id)
);

CREATE INDEX deliveries_lease_idx ON deliveries (leased_until);
