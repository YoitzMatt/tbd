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

	mu    sync.Mutex
	conns map[uint64]*clientConn
}

type clientConn struct {
	net.Conn
	writeMu sync.Mutex
}

func (c *clientConn) writeFrame(frame protocol.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return protocol.Encode(c.Conn, frame)
}

func New(addr string, broker *pubsub.Broker, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{
		addr:   addr,
		broker: broker,
		log:    log,
		conns:  make(map[uint64]*clientConn),
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("server: listen: %w", err)
	}
	return s.Serve(ctx, ln)
}

// Serve accepts connections from ln until ctx is canceled or the listener fails.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	s.ln = ln
	s.log.Info("listening", "addr", ln.Addr().String())

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		rawConn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("server: accept: %w", err)
		}
		conn := &clientConn{Conn: rawConn}
		id := s.nextID.Add(1)
		s.track(id, conn)
		go s.handle(ctx, id, conn)
	}
}

func (s *Server) track(id uint64, c *clientConn) {
	s.mu.Lock()
	s.conns[id] = c
	s.mu.Unlock()
}

func (s *Server) untrack(id uint64) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}

func (s *Server) handle(ctx context.Context, id uint64, conn *clientConn) {
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

func (s *Server) dispatch(ctx context.Context, id uint64, conn *clientConn, frame protocol.Frame) error {
	switch frame.Type {
	case protocol.TypePing:
		return conn.writeFrame(protocol.Frame{Type: protocol.TypePong})

	case protocol.TypePublish:
		topic, body, err := protocol.DecodePublish(frame.Payload)
		if err != nil {
			return err
		}
		msgID, err := s.broker.Publish(ctx, topic, body)
		if err != nil {
			return err
		}
		return conn.writeFrame(protocol.Frame{
			Type:    protocol.TypePubOK,
			Payload: protocol.EncodePubOK(uint64(msgID)),
		})

	case protocol.TypeSubscribe:
		topic, group, err := protocol.DecodeSubscribe(frame.Payload)
		if err != nil {
			return err
		}
		deliver := func(message store.Message, topic string) error {
			err := conn.writeFrame(protocol.Frame{
				Type:    protocol.TypeMsg,
				Payload: protocol.EncodeMsg(uint64(message.ID), topic, message.Payload),
			})
			if err != nil {
				_ = conn.Close()
			}
			return err
		}
		if _, err := s.broker.Subscribe(ctx, id, topic, group, deliver); err != nil {
			return err
		}
		if err := conn.writeFrame(protocol.Frame{Type: protocol.TypeOK}); err != nil {
			s.broker.Unsubscribe(id)
			return err
		}
		return s.broker.Activate(ctx, id)

	case protocol.TypeUnsub:
		s.broker.Unsubscribe(id)
		return conn.writeFrame(protocol.Frame{Type: protocol.TypeOK})

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
		return conn.writeFrame(protocol.Frame{Type: protocol.TypeOK})

	default:
		return fmt.Errorf("unsupported type %s", frame.Type)
	}
}

func writeErr(conn *clientConn, code uint16, msg string) error {
	return conn.writeFrame(protocol.Frame{
		Type:    protocol.TypeErr,
		Payload: protocol.EncodeErr(code, msg),
	})
}
