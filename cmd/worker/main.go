package main

import (
	"context"
	"flag"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shubhendu-abhishek/tenant-control-plane/internal/config"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/messaging"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	defaultFailRate, err := config.Float("WORKER_FAIL_RATE", 0)
	if err != nil {
		slog.Error("invalid worker config", "error", err)
		os.Exit(2)
	}
	defaultMin, err := config.Int("WORKER_MIN_DELAY_MS", 500)
	if err != nil {
		slog.Error("invalid worker config", "error", err)
		os.Exit(2)
	}
	defaultMax, err := config.Int("WORKER_MAX_DELAY_MS", 2500)
	if err != nil {
		slog.Error("invalid worker config", "error", err)
		os.Exit(2)
	}
	failRate := flag.Float64("fail-rate", defaultFailRate, "probability of a failed provisioning task, in [0,1]")
	minDelay := flag.Int("min-delay-ms", defaultMin, "minimum simulated processing delay in milliseconds")
	maxDelay := flag.Int("max-delay-ms", defaultMax, "maximum simulated processing delay in milliseconds")
	flag.Parse()
	if math.IsNaN(*failRate) || *failRate < 0 || *failRate > 1 {
		slog.Error("--fail-rate must be between 0 and 1")
		os.Exit(2)
	}
	if *minDelay < 0 || *maxDelay < *minDelay {
		slog.Error("delay values must satisfy 0 <= min-delay-ms <= max-delay-ms")
		os.Exit(2)
	}
	url := config.String("AMQP_URL", "amqp://guest:guest@localhost:5672/")
	cfg := messaging.WorkerConfig{FailRate: *failRate, MinDelay: time.Duration(*minDelay) * time.Millisecond, MaxDelay: time.Duration(*maxDelay) * time.Millisecond}
	slog.Info("worker simulator starting", "fail_rate", cfg.FailRate, "min_delay_ms", *minDelay, "max_delay_ms", *maxDelay)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	messaging.ConsumeTasks(ctx, url, cfg)
}
