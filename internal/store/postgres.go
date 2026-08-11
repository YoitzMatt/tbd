package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres implements Store using PostgreSQL.
type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

func (p *Postgres) EnsureTopic(ctx context.Context, name string) (Topic, error) {
	const q = `
		INSERT INTO topics (name) VALUES ($1)
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id, name`
	var t Topic
	err := p.pool.QueryRow(ctx, q, name).Scan(&t.ID, &t.Name)
	return t, err
}

func (p *Postgres) Publish(ctx context.Context, topic string, payload []byte) (int64, error) {
	t, err := p.EnsureTopic(ctx, topic)
	if err != nil {
		return 0, err
	}
	const q = `INSERT INTO messages (topic_id, payload) VALUES ($1, $2) RETURNING id`
	var id int64
	err = p.pool.QueryRow(ctx, q, t.ID, payload).Scan(&id)
	return id, err
}

func (p *Postgres) EnsureSubscription(ctx context.Context, topic, group string) (Subscription, error) {
	t, err := p.EnsureTopic(ctx, topic)
	if err != nil {
		return Subscription{}, err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer tx.Rollback(ctx)

	const upsertSub = `
		INSERT INTO subscriptions (topic_id, name) VALUES ($1, $2)
		ON CONFLICT (topic_id, name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id, topic_id, name`
	var s Subscription
	if err := tx.QueryRow(ctx, upsertSub, t.ID, group).Scan(&s.ID, &s.TopicID, &s.Name); err != nil {
		return Subscription{}, err
	}

	const upsertOff = `
		INSERT INTO subscription_offsets (subscription_id, last_acked_id)
		VALUES ($1, 0)
		ON CONFLICT (subscription_id) DO NOTHING`
	if _, err := tx.Exec(ctx, upsertOff, s.ID); err != nil {
		return Subscription{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Subscription{}, err
	}
	return s, nil
}

func (p *Postgres) LastAcked(ctx context.Context, subscriptionID int64) (int64, error) {
	const q = `SELECT last_acked_id FROM subscription_offsets WHERE subscription_id = $1`
	var id int64
	err := p.pool.QueryRow(ctx, q, subscriptionID).Scan(&id)
	return id, err
}

func (p *Postgres) ClaimUndelivered(ctx context.Context, subscriptionID int64) ([]Message, error) {
	const q = `
		SELECT m.id, m.topic_id, m.payload, m.created_at
		FROM messages m
		JOIN subscriptions s ON s.topic_id = m.topic_id
		JOIN subscription_offsets o ON o.subscription_id = s.id
		LEFT JOIN deliveries d
			ON d.subscription_id = s.id AND d.message_id = m.id
		WHERE s.id = $1
			AND m.id > o.last_acked_id
			AND (d.message_id IS NULL OR d.leased_until <= now())
		ORDER BY m.id`

	rows, err := p.pool.Query(ctx, q, subscriptionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var message Message
		if err := rows.Scan(&message.ID, &message.TopicID, &message.Payload, &message.CreatedAt); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (p *Postgres) Ack(ctx context.Context, subscriptionID, messageID int64) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		DELETE FROM deliveries
		WHERE subscription_id = $1 AND message_id = $2`, subscriptionID, messageID); err != nil {
		return err
	}

	// Advance cursor only forward (simple contiguous high-water mark for v1).
	if _, err := tx.Exec(ctx, `
		UPDATE subscription_offsets
		SET last_acked_id = GREATEST(last_acked_id, $2)
		WHERE subscription_id = $1`, subscriptionID, messageID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
