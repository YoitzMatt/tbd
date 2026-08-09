package protocol_test

import (
	"bytes"
	"testing"

	"tbd/internal/protocol"
)

func TestFrameRoundTrip(t *testing.T) {
	payload := protocol.EncodePublish("orders", []byte(`{"id":1}`))
	var buf bytes.Buffer
	if err := protocol.Encode(&buf, protocol.Frame{Type: protocol.TypePublish, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	f, err := protocol.Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if f.Type != protocol.TypePublish {
		t.Fatalf("type = %v", f.Type)
	}
	topic, body, err := protocol.DecodePublish(f.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if topic != "orders" || string(body) != `{"id":1}` {
		t.Fatalf("got topic=%q body=%q", topic, body)
	}
}

func TestSubscribeAndMsgCodecs(t *testing.T) {
	p := protocol.EncodeSubscribe("orders", "billing")
	topic, group, err := protocol.DecodeSubscribe(p)
	if err != nil || topic != "orders" || group != "billing" {
		t.Fatalf("subscribe: %q %q %v", topic, group, err)
	}

	msg := protocol.EncodeMsg(42, "orders", []byte("hi"))
	id, topic, body, err := protocol.DecodeMsg(msg)
	if err != nil || id != 42 || topic != "orders" || string(body) != "hi" {
		t.Fatalf("msg: %d %q %q %v", id, topic, body, err)
	}
}
