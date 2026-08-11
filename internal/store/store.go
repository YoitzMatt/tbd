// Package store defines persistence for topics, messages, and subscriptions.
package store

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("store: not found")
)

type Topic struct {
	ID   int64
	Name string
}

type Message struct {
	ID        int64
	TopicID   int64
	Payload   []byte
	CreatedAt time.Time
}

type Subscription struct {
	ID      int64
	TopicID int64
	Name    string // consumer group
}

// Store is the durable backing store for the broker.
type Store interface {
	EnsureTopic(ctx context.Context, name string) (Topic, error)
	Publish(ctx context.Context, topic string, payload []byte) (msgID int64, err error)
	EnsureSubscription(ctx context.Context, topic, group string) (Subscription, error)
	LastAcked(ctx context.Context, subscriptionID int64) (int64, error)
	ClaimUndelivered(ctx context.Context, subscriptionID int64) ([]Message, error)
	Ack(ctx context.Context, subscriptionID, messageID int64) error
	Close() error
}
