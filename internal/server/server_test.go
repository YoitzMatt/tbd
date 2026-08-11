package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"tbd/internal/protocol"
	"tbd/internal/pubsub"
	"tbd/internal/store"
)

func TestClientConnSerializesFrames(t *testing.T) {
	raw := &bufferConn{}
	conn := &clientConn{Conn: raw}

	const count = 20
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := conn.writeFrame(protocol.Frame{
				Type:    protocol.TypeMsg,
				Payload: protocol.EncodeMsg(uint64(id), "orders", []byte("body")),
			}); err != nil {
				t.Errorf("write frame: %v", err)
			}
		}(i)
	}
	wg.Wait()

	reader := bytes.NewReader(raw.Bytes())
	seen := make(map[uint64]bool, count)
	for i := 0; i < count; i++ {
		frame, err := protocol.Decode(reader)
		if err != nil {
			t.Fatalf("decode frame %d: %v", i, err)
		}
		id, _, _, err := protocol.DecodeMsg(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("decoded %d unique frames, want %d", len(seen), count)
	}
}

func TestClientConnReturnsWriteFailure(t *testing.T) {
	writeErr := errors.New("write failed")
	conn := &clientConn{Conn: &failingConn{err: writeErr}}
	err := conn.writeFrame(protocol.Frame{Type: protocol.TypeOK})
	if !errors.Is(err, writeErr) {
		t.Fatalf("write error = %v, want %v", err, writeErr)
	}
}

func TestSubscribePublishReceiveMSG(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DATABASE_URL is not set; skipping TCP/Postgres integration test")
	}

	ctx := context.Background()
	st, err := store.NewPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect store: %v", err)
	}
	defer st.Close()

	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect cleanup pool: %v", err)
	}
	defer admin.Close()

	topic := fmt.Sprintf("server-integration-%d", time.Now().UnixNano())
	defer cleanupIntegrationTopic(context.Background(), t, admin, topic)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	broker := pubsub.New(st, log)
	srv := New("", broker, log)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Serve(serverCtx, ln)
	}()
	defer func() {
		cancel()
		_ = ln.Close()
		if err := <-serverDone; err != nil {
			t.Errorf("server stopped: %v", err)
		}
	}()

	subscriber, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Close()
	if err := subscriber.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := protocol.Encode(subscriber, protocol.Frame{
		Type:    protocol.TypeSubscribe,
		Payload: protocol.EncodeSubscribe(topic, "billing"),
	}); err != nil {
		t.Fatal(err)
	}
	subResponse, err := protocol.Decode(subscriber)
	if err != nil {
		t.Fatal(err)
	}
	if subResponse.Type != protocol.TypeOK {
		t.Fatalf("subscribe response = %s, want OK", subResponse.Type)
	}

	publisher, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	if err := publisher.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"order_id":42}`)
	if err := protocol.Encode(publisher, protocol.Frame{
		Type:    protocol.TypePublish,
		Payload: protocol.EncodePublish(topic, body),
	}); err != nil {
		t.Fatal(err)
	}

	publishResponse, err := protocol.Decode(publisher)
	if err != nil {
		t.Fatal(err)
	}
	if publishResponse.Type != protocol.TypePubOK {
		t.Fatalf("publish response = %s, want PUB_OK", publishResponse.Type)
	}
	publishedID, err := protocol.DecodePubOK(publishResponse.Payload)
	if err != nil {
		t.Fatal(err)
	}

	messageFrame, err := protocol.Decode(subscriber)
	if err != nil {
		t.Fatal(err)
	}
	if messageFrame.Type != protocol.TypeMsg {
		t.Fatalf("subscriber frame = %s, want MSG", messageFrame.Type)
	}
	messageID, messageTopic, messageBody, err := protocol.DecodeMsg(messageFrame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if messageID != publishedID || messageTopic != topic || !bytes.Equal(messageBody, body) {
		t.Fatalf(
			"MSG = id %d topic %q body %q, want id %d topic %q body %q",
			messageID, messageTopic, messageBody, publishedID, topic, body,
		)
	}
}

type bufferConn struct {
	bytes.Buffer
}

func (c *bufferConn) Close() error                     { return nil }
func (c *bufferConn) LocalAddr() net.Addr              { return testAddr("local") }
func (c *bufferConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (c *bufferConn) SetDeadline(time.Time) error      { return nil }
func (c *bufferConn) SetReadDeadline(time.Time) error  { return nil }
func (c *bufferConn) SetWriteDeadline(time.Time) error { return nil }

type failingConn struct {
	err error
}

func (c *failingConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *failingConn) Write([]byte) (int, error)        { return 0, c.err }
func (c *failingConn) Close() error                     { return nil }
func (c *failingConn) LocalAddr() net.Addr              { return testAddr("local") }
func (c *failingConn) RemoteAddr() net.Addr             { return testAddr("remote") }
func (c *failingConn) SetDeadline(time.Time) error      { return nil }
func (c *failingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *failingConn) SetWriteDeadline(time.Time) error { return nil }

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func cleanupIntegrationTopic(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	topic string,
) {
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
		if _, err := pool.Exec(ctx, query, topic); err != nil {
			t.Errorf("cleanup topic %q: %v", topic, err)
		}
	}
}
