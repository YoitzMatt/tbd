package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tbd/internal/pubsub"
	"tbd/internal/server"
	"tbd/internal/store"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	addr := envOr("BROKER_ADDR", ":9000")
	dbURL := envOr("DATABASE_URL", "postgres://pubsub@localhost:5433/pubsub?sslmode=disable")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.NewPostgres(ctx, dbURL)
	if err != nil {
		log.Error("store init failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	broker := pubsub.New(st, log)
	srv := server.New(addr, broker, log)

	log.Info("broker starting", "addr", addr)
	if err := srv.ListenAndServe(ctx); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
