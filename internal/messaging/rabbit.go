package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/domain"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/store"
)

const (
	TaskQueue     = store.TaskTopic
	ProgressQueue = "provisioning.progress"
	ProgressDLQ   = "provisioning.progress.dlq"
)

type Publisher struct{ URL string }

func (p Publisher) Publish(ctx context.Context, routingKey, messageID string, body []byte) error {
	conn, err := amqp.Dial(p.URL)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	defer ch.Close()
	if err := ensureTopology(ch); err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := ch.PublishWithContext(ctx, "", routingKey, false, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent,
		MessageId: messageID, Timestamp: time.Now().UTC(), Body: body,
	}); err != nil {
		return fmt.Errorf("publish %s: %w", routingKey, err)
	}
	select {
	case confirmation, ok := <-confirms:
		if !ok {
			return errors.New("RabbitMQ publisher confirmation channel closed")
		}
		if !confirmation.Ack {
			return errors.New("RabbitMQ negatively acknowledged publication")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for RabbitMQ publisher confirmation")
	}
}

func ensureTopology(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(TaskQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare task queue: %w", err)
	}
	if _, err := ch.QueueDeclare(ProgressDLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare progress dead-letter queue: %w", err)
	}
	args := amqp.Table{"x-dead-letter-exchange": "", "x-dead-letter-routing-key": ProgressDLQ}
	if _, err := ch.QueueDeclare(ProgressQueue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare progress queue: %w", err)
	}
	return nil
}

func (p Publisher) PublishProgress(ctx context.Context, update domain.ProgressUpdate) error {
	body, err := json.Marshal(update)
	if err != nil {
		return err
	}
	return p.Publish(ctx, ProgressQueue, update.ID, body)
}

type ProgressHandler func(context.Context, domain.ProgressUpdate) error

// ConsumeProgress reconnects when RabbitMQ is unavailable. Invalid or poison
// payloads are dead-lettered; transient handler errors are retried with backoff.
func ConsumeProgress(ctx context.Context, url string, handler ProgressHandler) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := consumeProgressSession(ctx, url, handler); err != nil && ctx.Err() == nil {
			slog.Error("progress consumer disconnected", "error", err, "retry_in", backoff)
		}
		if !sleepContext(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, 15*time.Second)
	}
}

func consumeProgressSession(ctx context.Context, url string, handler ProgressHandler) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ensureTopology(ch); err != nil {
		return err
	}
	if err := ch.Qos(10, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(ProgressQueue, "control-plane", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("progress delivery channel closed")
			}
			var update domain.ProgressUpdate
			if err := json.Unmarshal(delivery.Body, &update); err != nil || update.ID == "" || update.TaskID == "" || !validProgressStatus(update.Status) {
				logPoison("progress", delivery.MessageId, err)
				_ = delivery.Nack(false, false)
				continue
			}
			attempt := 0
			for {
				handlerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := handler(handlerCtx, update)
				cancel()
				if err == nil {
					_ = delivery.Ack(false)
					break
				}
				if store.ErrorCode(err) == "task_not_found" || store.ErrorCode(err) == "invalid_progress_update" {
					logPoison("progress", delivery.MessageId, err)
					_ = delivery.Nack(false, false)
					break
				}
				attempt++
				delay := retryDelay(attempt)
				slog.Warn("failed to apply progress update; retrying", "update_id", update.ID, "attempt", attempt, "retry_in", delay, "error", err)
				if !sleepContext(ctx, delay) {
					return nil
				}
				if ctx.Err() != nil {
					return nil
				}
			}
		}
	}
}

func validProgressStatus(status domain.TaskStatus) bool {
	return status == domain.TaskInProgress || status == domain.TaskDone || status == domain.TaskFailed
}

func retryDelay(attempt int) time.Duration {
	if attempt > 6 {
		attempt = 6
	}
	return time.Duration(1<<uint(attempt-1)) * time.Second
}
func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func logPoison(kind, messageID string, err error) {
	if err != nil {
		slog.Error("dead-lettering invalid message", "kind", kind, "message_id", messageID, "error", err)
	} else {
		slog.Error("dead-lettering invalid message", "kind", kind, "message_id", messageID)
	}
}

type WorkerConfig struct {
	FailRate float64
	MinDelay time.Duration
	MaxDelay time.Duration
}

func ConsumeTasks(ctx context.Context, url string, cfg WorkerConfig) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := consumeTaskSession(ctx, url, cfg); err != nil && ctx.Err() == nil {
			slog.Error("worker disconnected", "error", err, "retry_in", backoff)
		}
		if !sleepContext(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, 15*time.Second)
	}
}

func consumeTaskSession(ctx context.Context, url string, cfg WorkerConfig) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ensureTopology(ch); err != nil {
		return err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	deliveries, err := ch.Consume(TaskQueue, "provisioning-worker", false, false, false, false, nil)
	if err != nil {
		return err
	}
	publisher := Publisher{URL: url}
	attempts := make(map[string]int)
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("task delivery channel closed")
			}
			var task domain.Task
			if err := json.Unmarshal(delivery.Body, &task); err != nil || task.ID == "" || task.TenantID == "" || (task.Type != domain.TaskDeploy && task.Type != domain.TaskUpdate && task.Type != domain.TaskDestroy) {
				logPoison("task", delivery.MessageId, err)
				_ = delivery.Nack(false, false)
				continue
			}
			if task.Status != domain.TaskAccepted {
				_ = delivery.Ack(false)
				continue
			}
			if err := processTask(ctx, publisher, task, cfg); err != nil {
				attempts[task.ID]++
				delay := retryDelay(attempts[task.ID])
				slog.Warn("task processing failed; requeueing", "task_id", task.ID, "attempt", attempts[task.ID], "retry_in", delay, "error", err)
				_ = delivery.Nack(false, true)
				if !sleepContext(ctx, delay) {
					return nil
				}
				continue
			}
			delete(attempts, task.ID)
			_ = delivery.Ack(false)
		}
	}
}

type ProgressPublisher interface {
	PublishProgress(context.Context, domain.ProgressUpdate) error
}

func processTask(ctx context.Context, publisher ProgressPublisher, task domain.Task, cfg WorkerConfig) error {
	now := domain.NewTimestamp(time.Now())
	progress := domain.ProgressUpdate{ID: task.ID + ":in_progress", TaskID: task.ID, Status: domain.TaskInProgress, OccurredAt: now}
	if err := publisher.PublishProgress(ctx, progress); err != nil {
		return fmt.Errorf("publish in_progress: %w", err)
	}
	delay := cfg.MinDelay
	if cfg.MaxDelay > cfg.MinDelay {
		span := cfg.MaxDelay - cfg.MinDelay
		delay += time.Duration(rand.Int64N(int64(span) + 1))
	}
	if !sleepContext(ctx, delay) {
		return ctx.Err()
	}
	status := domain.TaskDone
	if rand.Float64() < cfg.FailRate {
		status = domain.TaskFailed
	}
	terminal := domain.ProgressUpdate{ID: task.ID + ":terminal", TaskID: task.ID, Status: status, OccurredAt: domain.NewTimestamp(time.Now())}
	if err := publisher.PublishProgress(ctx, terminal); err != nil {
		return fmt.Errorf("publish %s: %w", status, err)
	}
	slog.Info("simulated task complete", "task_id", task.ID, "tenant_id", task.TenantID, "task_type", task.Type, "status", status, "delay", delay)
	return nil
}
