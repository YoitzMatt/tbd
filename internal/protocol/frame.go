// Package protocol defines the length-prefixed TCP frame format and message types
// used between clients and the broker.
//
// Wire format:
//
//	[u32 big-endian length][u8 type][payload...]
//
// Length covers type + payload (not including the 4-byte length prefix).
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	MaxFrameSize = 1 << 20 // 1 MiB
	headerSize   = 4
	typeSize     = 1
)

var (
	ErrFrameTooLarge = errors.New("protocol: frame exceeds max size")
	ErrShortBuffer   = errors.New("protocol: short buffer")
)

// Type identifies a frame's message kind.
type Type uint8

const (
	TypePublish   Type = 1
	TypeSubscribe Type = 2
	TypeUnsub     Type = 3
	TypeAck       Type = 4
	TypePing      Type = 5

	TypePubOK Type = 10
	TypeMsg   Type = 11
	TypeOK    Type = 12
	TypeErr   Type = 13
	TypePong  Type = 14
)

func (t Type) String() string {
	switch t {
	case TypePublish:
		return "PUBLISH"
	case TypeSubscribe:
		return "SUBSCRIBE"
	case TypeUnsub:
		return "UNSUBSCRIBE"
	case TypeAck:
		return "ACK"
	case TypePing:
		return "PING"
	case TypePubOK:
		return "PUB_OK"
	case TypeMsg:
		return "MSG"
	case TypeOK:
		return "OK"
	case TypeErr:
		return "ERR"
	case TypePong:
		return "PONG"
	default:
		return fmt.Sprintf("Type(%d)", t)
	}
}

// Frame is one protocol message on the wire.
type Frame struct {
	Type    Type
	Payload []byte
}

// Encode writes a single frame to w.
func Encode(w io.Writer, f Frame) error {
	n := typeSize + len(f.Payload)
	if n > MaxFrameSize {
		return ErrFrameTooLarge
	}
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(n))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write([]byte{byte(f.Type)}); err != nil {
		return err
	}
	if len(f.Payload) == 0 {
		return nil
	}
	_, err := w.Write(f.Payload)
	return err
}

// Decode reads one frame from r.
func Decode(r io.Reader) (Frame, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n < typeSize || n > MaxFrameSize {
		return Frame{}, ErrFrameTooLarge
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Frame{}, err
	}
	return Frame{
		Type:    Type(buf[0]),
		Payload: buf[1:],
	}, nil
}
