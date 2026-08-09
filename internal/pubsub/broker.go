// Package pubsub implements broker orchestration on top of the durable store.
// Connection-level subscribe state lives in memory; durability is in Store.
package pubsub

import (
	"context"
	"log/slog"
	"sync"

	"tbd/internal/store"
)

// Broker coordinates publish/subscribe against a Store.
type Broker struct {
	store store.Store
	log   *slog.Logger

	mu   sync.RWMutex
	subs map[connKey]*activeSub // live TCP subscriptions
}

type connKey struct {
	connID uint64
}

type activeSub struct {
	Subscription store.Subscription
	Topic        string
	Group        string
}

func New(st store.Store, log *slog.Logger) *Broker {
	if log == nil {
		log = slog.Default()
	}
	return &Broker{
		store: st,
		log:   log,
		subs:  make(map[connKey]*activeSub),
	}
}

func (b *Broker) Publish(ctx context.Context, topic string, payload []byte) (int64, error) {
	id, err := b.store.Publish(ctx, topic, payload)
	if err != nil {
		return 0, err
	}
	b.log.Info("published", "topic", topic, "message_id", id)
	// Delivery to live subscribers lands in a later milestone.
	return id, nil
}

func (b *Broker) Subscribe(ctx context.Context, connID uint64, topic, group string) (store.Subscription, error) {
	sub, err := b.store.EnsureSubscription(ctx, topic, group)
	if err != nil {
		return store.Subscription{}, err
	}
	b.mu.Lock()
	b.subs[connKey{connID: connID}] = &activeSub{
		Subscription: sub,
		Topic:        topic,
		Group:        group,
	}
	b.mu.Unlock()
	b.log.Info("subscribed", "conn", connID, "topic", topic, "group", group, "subscription_id", sub.ID)
	return sub, nil
}

// Unsubscribe drops the live connection mapping. Durable subscription rows remain.
func (b *Broker) Unsubscribe(connID uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.subs[connKey{connID: connID}]; ok {
		b.log.Info("unsubscribed", "conn", connID, "topic", s.Topic, "group", s.Group)
		delete(b.subs, connKey{connID: connID})
	}
}

func (b *Broker) Ack(ctx context.Context, connID uint64, messageID int64) error {
	b.mu.RLock()
	s, ok := b.subs[connKey{connID: connID}]
	b.mu.RUnlock()
	if !ok {
		return store.ErrNotFound
	}
	return b.store.Ack(ctx, s.Subscription.ID, messageID)
}

func (b *Broker) ActiveSubscription(connID uint64) (topic, group string, ok bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	s, ok := b.subs[connKey{connID: connID}]
	if !ok {
		return "", "", false
	}
	return s.Topic, s.Group, true
}
