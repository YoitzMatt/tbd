// Package pubsub implements broker orchestration on top of the durable store.
// Connection-level subscribe state lives in memory; durability is in Store.
package pubsub

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"tbd/internal/store"
)

// DeliveryFunc writes a message to one live subscriber.
type DeliveryFunc func(message store.Message, topic string) error

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
	Deliver      DeliveryFunc

	deliveryMu sync.Mutex
	enabled    bool
	sent       map[int64]struct{}
	closed     atomic.Bool
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

	b.mu.RLock()
	var subscribers []*activeSub
	for _, sub := range b.subs {
		if sub.Topic == topic && !sub.closed.Load() {
			subscribers = append(subscribers, sub)
		}
	}
	b.mu.RUnlock()

	for _, sub := range subscribers {
		if err := b.deliverPending(ctx, sub); err != nil {
			b.log.Warn("deliver after publish failed",
				"topic", topic,
				"subscription_id", sub.Subscription.ID,
				"err", err,
			)
		}
	}
	return id, nil
}

func (b *Broker) Subscribe(
	ctx context.Context,
	connID uint64,
	topic, group string,
	deliver DeliveryFunc,
) (store.Subscription, error) {
	sub, err := b.store.EnsureSubscription(ctx, topic, group)
	if err != nil {
		return store.Subscription{}, err
	}
	active := &activeSub{
		Subscription: sub,
		Topic:        topic,
		Group:        group,
		Deliver:      deliver,
		sent:         make(map[int64]struct{}),
	}
	b.mu.Lock()
	key := connKey{connID: connID}
	if previous, ok := b.subs[key]; ok {
		previous.closed.Store(true)
	}
	b.subs[key] = active
	b.mu.Unlock()
	b.log.Info("subscribed", "conn", connID, "topic", topic, "group", group, "subscription_id", sub.ID)
	return sub, nil
}

// Activate enables push delivery after the server has sent the SUBSCRIBE response.
func (b *Broker) Activate(ctx context.Context, connID uint64) error {
	b.mu.RLock()
	sub, ok := b.subs[connKey{connID: connID}]
	b.mu.RUnlock()
	if !ok {
		return store.ErrNotFound
	}

	sub.deliveryMu.Lock()
	defer sub.deliveryMu.Unlock()
	if sub.closed.Load() {
		return store.ErrNotFound
	}
	sub.enabled = true
	return b.deliverPendingLocked(ctx, sub)
}

// Unsubscribe drops the live connection mapping. Durable subscription rows remain.
func (b *Broker) Unsubscribe(connID uint64) {
	b.mu.Lock()
	key := connKey{connID: connID}
	sub, ok := b.subs[key]
	if ok {
		delete(b.subs, key)
	}
	b.mu.Unlock()
	if !ok {
		return
	}
	sub.closed.Store(true)
	b.log.Info("unsubscribed", "conn", connID, "topic", sub.Topic, "group", sub.Group)
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

func (b *Broker) deliverPending(ctx context.Context, sub *activeSub) error {
	sub.deliveryMu.Lock()
	defer sub.deliveryMu.Unlock()
	return b.deliverPendingLocked(ctx, sub)
}

func (b *Broker) deliverPendingLocked(ctx context.Context, sub *activeSub) error {
	if sub.closed.Load() || !sub.enabled {
		return nil
	}

	messages, err := b.store.ClaimUndelivered(ctx, sub.Subscription.ID)
	if err != nil {
		return err
	}
	for _, message := range messages {
		if _, alreadySent := sub.sent[message.ID]; alreadySent {
			continue
		}
		if err := sub.Deliver(message, sub.Topic); err != nil {
			sub.closed.Store(true)
			b.removeActive(sub)
			return err
		}
		sub.sent[message.ID] = struct{}{}
	}
	return nil
}

func (b *Broker) removeActive(target *activeSub) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, sub := range b.subs {
		if sub == target {
			delete(b.subs, key)
			return
		}
	}
}
