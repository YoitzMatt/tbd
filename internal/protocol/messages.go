package protocol

import (
	"encoding/binary"
	"errors"
)

var ErrBadPayload = errors.New("protocol: malformed payload")

// EncodePublish builds a PUBLISH payload: [u16 topic_len][topic][payload...]
func EncodePublish(topic string, body []byte) []byte {
	return encodeNamed(topic, body)
}

// DecodePublish parses a PUBLISH payload.
func DecodePublish(p []byte) (topic string, body []byte, err error) {
	return decodeNamed(p)
}

// EncodeSubscribe builds a SUBSCRIBE payload: [u16 topic_len][topic][u16 group_len][group]
func EncodeSubscribe(topic, group string) []byte {
	return encodeTwoStrings(topic, group)
}

// DecodeSubscribe parses a SUBSCRIBE payload.
func DecodeSubscribe(p []byte) (topic, group string, err error) {
	return decodeTwoStrings(p)
}

// EncodeUnsub builds an UNSUBSCRIBE payload: [u16 topic_len][topic][u16 group_len][group]
func EncodeUnsub(topic, group string) []byte {
	return encodeTwoStrings(topic, group)
}

// DecodeUnsub parses an UNSUBSCRIBE payload.
func DecodeUnsub(p []byte) (topic, group string, err error) {
	return decodeTwoStrings(p)
}

// EncodeAck builds an ACK payload: [u64 message_id]
func EncodeAck(messageID uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], messageID)
	return b[:]
}

// DecodeAck parses an ACK payload.
func DecodeAck(p []byte) (uint64, error) {
	if len(p) < 8 {
		return 0, ErrBadPayload
	}
	return binary.BigEndian.Uint64(p[:8]), nil
}

// EncodePubOK builds a PUB_OK payload: [u64 message_id]
func EncodePubOK(messageID uint64) []byte {
	return EncodeAck(messageID)
}

// DecodePubOK parses a PUB_OK payload.
func DecodePubOK(p []byte) (uint64, error) {
	return DecodeAck(p)
}

// EncodeMsg builds a MSG payload: [u64 id][u16 topic_len][topic][body...]
func EncodeMsg(id uint64, topic string, body []byte) []byte {
	tb := []byte(topic)
	out := make([]byte, 8+2+len(tb)+len(body))
	binary.BigEndian.PutUint64(out[0:8], id)
	binary.BigEndian.PutUint16(out[8:10], uint16(len(tb)))
	copy(out[10:], tb)
	copy(out[10+len(tb):], body)
	return out
}

// DecodeMsg parses a MSG payload.
func DecodeMsg(p []byte) (id uint64, topic string, body []byte, err error) {
	if len(p) < 10 {
		return 0, "", nil, ErrBadPayload
	}
	id = binary.BigEndian.Uint64(p[0:8])
	n := int(binary.BigEndian.Uint16(p[8:10]))
	if 10+n > len(p) {
		return 0, "", nil, ErrBadPayload
	}
	topic = string(p[10 : 10+n])
	body = p[10+n:]
	return id, topic, body, nil
}

// EncodeErr builds an ERR payload: [u16 code][message...]
func EncodeErr(code uint16, message string) []byte {
	mb := []byte(message)
	out := make([]byte, 2+len(mb))
	binary.BigEndian.PutUint16(out[0:2], code)
	copy(out[2:], mb)
	return out
}

// DecodeErr parses an ERR payload.
func DecodeErr(p []byte) (code uint16, message string, err error) {
	if len(p) < 2 {
		return 0, "", ErrBadPayload
	}
	return binary.BigEndian.Uint16(p[0:2]), string(p[2:]), nil
}

func encodeNamed(name string, body []byte) []byte {
	nb := []byte(name)
	out := make([]byte, 2+len(nb)+len(body))
	binary.BigEndian.PutUint16(out[0:2], uint16(len(nb)))
	copy(out[2:], nb)
	copy(out[2+len(nb):], body)
	return out
}

func decodeNamed(p []byte) (string, []byte, error) {
	if len(p) < 2 {
		return "", nil, ErrBadPayload
	}
	n := int(binary.BigEndian.Uint16(p[0:2]))
	if 2+n > len(p) {
		return "", nil, ErrBadPayload
	}
	return string(p[2 : 2+n]), p[2+n:], nil
}

func encodeTwoStrings(a, b string) []byte {
	ab, bb := []byte(a), []byte(b)
	out := make([]byte, 2+len(ab)+2+len(bb))
	binary.BigEndian.PutUint16(out[0:2], uint16(len(ab)))
	copy(out[2:], ab)
	off := 2 + len(ab)
	binary.BigEndian.PutUint16(out[off:off+2], uint16(len(bb)))
	copy(out[off+2:], bb)
	return out
}

func decodeTwoStrings(p []byte) (string, string, error) {
	if len(p) < 2 {
		return "", "", ErrBadPayload
	}
	n1 := int(binary.BigEndian.Uint16(p[0:2]))
	if 2+n1+2 > len(p) {
		return "", "", ErrBadPayload
	}
	a := string(p[2 : 2+n1])
	rest := p[2+n1:]
	n2 := int(binary.BigEndian.Uint16(rest[0:2]))
	if 2+n2 > len(rest) {
		return "", "", ErrBadPayload
	}
	b := string(rest[2 : 2+n2])
	return a, b, nil
}
