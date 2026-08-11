package pubsub_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"tbd/internal/pubsub"
	"tbd/internal/store"
)

func TestSubscribeActivationAndPublishDeliverPendingInOrder(t *testing.T) {
	st := &fakeStore{
		subscription: store.Subscription{ID: 7, TopicID: 3, Name: "billing"},
		messages: []store.Message{
			{ID: 1, TopicID: 3, Payload: []byte("one")},
			{ID: 2, TopicID: 3, Payload: []byte("two")},
		},
	}
	broker := pubsub.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))

	var (
		deliveredMu sync.Mutex
		delivered   []int64
	)
	deliver := func(message store.Message, topic string) error {
		if topic != "orders" {
			t.Fatalf("topic = %q, want orders", topic)
		}
		deliveredMu.Lock()
		delivered = append(delivered, message.ID)
		deliveredMu.Unlock()
		return nil
	}

	ctx := context.Background()
	if _, err := broker.Subscribe(ctx, 1, "orders", "billing", deliver); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Publish(ctx, "orders", []byte("three")); err != nil {
		t.Fatal(err)
	}
	if len(delivered) != 0 {
		t.Fatalf("delivered before activation: %v", delivered)
	}

	if err := broker.Activate(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Publish(ctx, "orders", []byte("four")); err != nil {
		t.Fatal(err)
	}

	deliveredMu.Lock()
	defer deliveredMu.Unlock()
	want := []int64{1, 2, 3, 4}
	if len(delivered) != len(want) {
		t.Fatalf("delivered = %v, want %v", delivered, want)
	}
	for i := range want {
		if delivered[i] != want[i] {
			t.Fatalf("delivered = %v, want %v", delivered, want)
		}
	}
}

func TestDeliveryFailureRemovesActiveSubscription(t *testing.T) {
	st := &fakeStore{
		subscription: store.Subscription{ID: 7, TopicID: 3, Name: "billing"},
		messages:     []store.Message{{ID: 1, TopicID: 3, Payload: []byte("one")}},
	}
	broker := pubsub.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	writeErr := errors.New("write failed")

	if _, err := broker.Subscribe(context.Background(), 1, "orders", "billing",
		func(store.Message, string) error { return writeErr },
	); err != nil {
		t.Fatal(err)
	}
	if err := broker.Activate(context.Background(), 1); !errors.Is(err, writeErr) {
		t.Fatalf("Activate error = %v, want %v", err, writeErr)
	}
	if _, _, ok := broker.ActiveSubscription(1); ok {
		t.Fatal("subscription remains active after delivery failure")
	}
}

type fakeStore struct {
	mu           sync.Mutex
	subscription store.Subscription
	messages     []store.Message
	lastAcked    int64
}

func (f *fakeStore) EnsureTopic(context.Context, string) (store.Topic, error) {
	return store.Topic{ID: f.subscription.TopicID, Name: "orders"}, nil
}

func (f *fakeStore) Publish(_ context.Context, _ string, payload []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := int64(len(f.messages) + 1)
	f.messages = append(f.messages, store.Message{
		ID:        id,
		TopicID:   f.subscription.TopicID,
		Payload:   append([]byte(nil), payload...),
		CreatedAt: time.Now(),
	})
	return id, nil
}

func (f *fakeStore) EnsureSubscription(context.Context, string, string) (store.Subscription, error) {
	return f.subscription, nil
}

func (f *fakeStore) LastAcked(context.Context, int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastAcked, nil
}

func (f *fakeStore) ClaimUndelivered(context.Context, int64) ([]store.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var messages []store.Message
	for _, message := range f.messages {
		if message.ID > f.lastAcked {
			message.Payload = append([]byte(nil), message.Payload...)
			messages = append(messages, message)
		}
	}
	return messages, nil
}

func (f *fakeStore) Ack(_ context.Context, _ int64, messageID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if messageID > f.lastAcked {
		f.lastAcked = messageID
	}
	return nil
}

func (f *fakeStore) Close() error { return nil }
