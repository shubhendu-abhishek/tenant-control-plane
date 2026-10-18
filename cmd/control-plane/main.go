package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shubhendu-abhishek/tenant-control-plane/internal/api"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/config"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/domain"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/messaging"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	databaseURL := config.String("DATABASE_URL", "postgres://controlplane:local-dev-only-change-me@localhost:5432/controlplane?sslmode=disable")
	amqpURL := config.String("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	httpAddr := config.String("HTTP_ADDR", ":8080")

	st, err := openWithRetry(databaseURL, 30*time.Second)
	if err != nil {
		slog.Error("database unavailable", "error", err)
		os.Exit(1)
	}
	defer st.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := st.Migrate(ctx); err != nil {
		slog.Error("database migration failed", "error", err)
		os.Exit(1)
	}

	publisher := messaging.Publisher{URL: amqpURL}
	go dispatchOutbox(ctx, st, publisher)
	go messaging.ConsumeProgress(ctx, amqpURL, func(updateCtx context.Context, update domain.ProgressUpdate) error {
		return st.ApplyProgress(updateCtx, update)
	})

	server := &http.Server{Addr: httpAddr, Handler: api.New(st).Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		slog.Info("control plane listening", "addr", httpAddr)
		serverErr <- server.ListenAndServe()
	}()
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server stopped", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(drainCtx); err != nil {
			slog.Error("HTTP shutdown failed", "error", err)
		}
	}
}

func openWithRetry(databaseURL string, maxWait time.Duration) (*store.Store, error) {
	deadline := time.Now().Add(maxWait)
	var lastErr error
	for time.Now().Before(deadline) {
		st, err := store.Open(databaseURL)
		if err == nil {
			return st, nil
		}
		lastErr = err
		slog.Warn("waiting for database", "error", err)
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

func dispatchOutbox(ctx context.Context, st *store.Store, publisher messaging.Publisher) {
	for ctx.Err() == nil {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		event, err := st.ClaimOutbox(queryCtx)
		cancel()
		if err != nil {
			slog.Error("claim outbox event", "error", err)
			if !sleepContext(ctx, time.Second) {
				return
			}
			continue
		}
		if event == nil {
			if !sleepContext(ctx, 250*time.Millisecond) {
				return
			}
			continue
		}
		publishCtx, publishCancel := context.WithTimeout(ctx, 8*time.Second)
		err = publisher.Publish(publishCtx, event.Topic, event.EventID, event.Payload)
		publishCancel()
		if err == nil {
			markCtx, markCancel := context.WithTimeout(ctx, 5*time.Second)
			err = st.MarkOutboxPublished(markCtx, event.ID)
			markCancel()
			if err != nil {
				slog.Error("mark outbox event published; event may be replayed", "event_id", event.EventID, "error", err)
			} else {
				slog.Info("published task event", "event_id", event.EventID, "attempt", event.Attempts)
			}
			continue
		}
		delay := time.Duration(1<<min(event.Attempts-1, 6)) * time.Second
		if delay > time.Minute {
			delay = time.Minute
		}
		markCtx, markCancel := context.WithTimeout(ctx, 5*time.Second)
		markErr := st.MarkOutboxFailed(markCtx, event.ID, err, delay)
		markCancel()
		slog.Warn("outbox publication failed", "event_id", event.EventID, "error", err, "mark_error", markErr, "retry_in", delay)
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
