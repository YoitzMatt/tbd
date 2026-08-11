package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestClaimUndeliveredFiltersCursorAndLeases(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping Postgres integration test")
	}

	ctx := context.Background()
	st, err := NewPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect to Postgres: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	topic := fmt.Sprintf("claim-undelivered-%d", time.Now().UnixNano())
	group := "test-consumer"
	t.Cleanup(func() {
		cleanupTopic(context.Background(), t, st, topic)
	})

	sub, err := st.EnsureSubscription(ctx, topic, group)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, payload := range []string{"one", "two", "three"} {
		id, err := st.Publish(ctx, topic, []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := st.Ack(ctx, sub.ID, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `
		INSERT INTO deliveries (subscription_id, message_id, leased_until)
		VALUES ($1, $2, now() + interval '1 hour'),
		       ($1, $3, now() - interval '1 hour')`,
		sub.ID, ids[1], ids[2],
	); err != nil {
		t.Fatal(err)
	}

	messages, err := st.ClaimUndelivered(ctx, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertMessageIDs(t, messages, []int64{ids[2]})

	if _, err := st.pool.Exec(ctx, `DELETE FROM deliveries WHERE subscription_id = $1`, sub.ID); err != nil {
		t.Fatal(err)
	}
	messages, err = st.ClaimUndelivered(ctx, sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertMessageIDs(t, messages, []int64{ids[1], ids[2]})
}

func assertMessageIDs(t *testing.T, messages []Message, want []int64) {
	t.Helper()
	if len(messages) != len(want) {
		t.Fatalf("message IDs = %v, want %v", messageIDs(messages), want)
	}
	for i := range want {
		if messages[i].ID != want[i] {
			t.Fatalf("message IDs = %v, want %v", messageIDs(messages), want)
		}
	}
}

func messageIDs(messages []Message) []int64 {
	ids := make([]int64, len(messages))
	for i, message := range messages {
		ids[i] = message.ID
	}
	return ids
}

func cleanupTopic(ctx context.Context, t *testing.T, st *Postgres, topic string) {
	t.Helper()
	for _, query := range []string{
		`DELETE FROM deliveries WHERE subscription_id IN (
			SELECT s.id FROM subscriptions s JOIN topics t ON t.id = s.topic_id WHERE t.name = $1
		)`,
		`DELETE FROM subscription_offsets WHERE subscription_id IN (
			SELECT s.id FROM subscriptions s JOIN topics t ON t.id = s.topic_id WHERE t.name = $1
		)`,
		`DELETE FROM subscriptions WHERE topic_id = (SELECT id FROM topics WHERE name = $1)`,
		`DELETE FROM messages WHERE topic_id = (SELECT id FROM topics WHERE name = $1)`,
		`DELETE FROM topics WHERE name = $1`,
	} {
		if _, err := st.pool.Exec(ctx, query, topic); err != nil {
			t.Errorf("cleanup topic %q: %v", topic, err)
		}
	}
}
