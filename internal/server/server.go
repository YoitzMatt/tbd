// Package server accepts TCP connections and dispatches protocol frames to the broker.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"tbd/internal/protocol"
	"tbd/internal/pubsub"
	"tbd/internal/store"
)

type Server struct {
	addr   string
	broker *pubsub.Broker
	log    *slog.Logger

	ln     net.Listener
	nextID atomic.Uint64

	mu   sync.Mutex
	conns map[uint64]net.Conn
}

func New(addr string, broker *pubsub.Broker, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		addr:   addr,
		broker: broker,
		log:    log,
		conns:  make(map[uint64]net.Conn),
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("server: listen: %w", err)
	}
	s.ln = ln
	s.log.Info("listening", "addr", ln.Addr().String())

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("server: accept: %w", err)
		}
		id := s.nextID.Add(1)
		s.track(id, conn)
		go s.handle(ctx, id, conn)
	}
}

func (s *Server) track(id uint64, c net.Conn) {
	s.mu.Lock()
	s.conns[id] = c
	s.mu.Unlock()
}

func (s *Server) untrack(id uint64) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}

func (s *Server) handle(ctx context.Context, id uint64, conn net.Conn) {
	defer func() {
		s.broker.Unsubscribe(id)
		s.untrack(id)
		_ = conn.Close()
		s.log.Info("connection closed", "conn", id)
	}()

	s.log.Info("connection opened", "conn", id, "remote", conn.RemoteAddr().String())

	for {
		frame, err := protocol.Decode(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			s.log.Warn("decode failed", "conn", id, "err", err)
			return
		}
		if err := s.dispatch(ctx, id, conn, frame); err != nil {
			s.log.Warn("dispatch failed", "conn", id, "type", frame.Type, "err", err)
			_ = writeErr(conn, 1, err.Error())
		}
	}
}

func (s *Server) dispatch(ctx context.Context, id uint64, conn net.Conn, frame protocol.Frame) error {
	switch frame.Type {
	case protocol.TypePing:
		return protocol.Encode(conn, protocol.Frame{Type: protocol.TypePong})

	case protocol.TypePublish:
		topic, body, err := protocol.DecodePublish(frame.Payload)
		if err != nil {
			return err
		}
		msgID, err := s.broker.Publish(ctx, topic, body)
		if err != nil {
			return err
		}
		return protocol.Encode(conn, protocol.Frame{
			Type:    protocol.TypePubOK,
			Payload: protocol.EncodePubOK(uint64(msgID)),
		})

	case protocol.TypeSubscribe:
		topic, group, err := protocol.DecodeSubscribe(frame.Payload)
		if err != nil {
			return err
		}
		if _, err := s.broker.Subscribe(ctx, id, topic, group); err != nil {
			return err
		}
		return protocol.Encode(conn, protocol.Frame{Type: protocol.TypeOK})

	case protocol.TypeUnsub:
		s.broker.Unsubscribe(id)
		return protocol.Encode(conn, protocol.Frame{Type: protocol.TypeOK})

	case protocol.TypeAck:
		msgID, err := protocol.DecodeAck(frame.Payload)
		if err != nil {
			return err
		}
		if err := s.broker.Ack(ctx, id, int64(msgID)); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("not subscribed")
			}
			return err
		}
		return protocol.Encode(conn, protocol.Frame{Type: protocol.TypeOK})

	default:
		return fmt.Errorf("unsupported type %s", frame.Type)
	}
}

func writeErr(conn net.Conn, code uint16, msg string) error {
	return protocol.Encode(conn, protocol.Frame{
		Type:    protocol.TypeErr,
		Payload: protocol.EncodeErr(code, msg),
	})
}
