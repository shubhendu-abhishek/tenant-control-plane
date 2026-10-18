package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/shubhendu-abhishek/tenant-control-plane/internal/domain"
)

type recordingPublisher struct{ updates []domain.ProgressUpdate }

func (p *recordingPublisher) PublishProgress(_ context.Context, update domain.ProgressUpdate) error {
	p.updates = append(p.updates, update)
	return nil
}

func TestWorkerPublishesStableProgressIDsAndConfigurableFailure(t *testing.T) {
	publisher := &recordingPublisher{}
	task := domain.Task{ID: "task-123", TenantID: "tenant-456", Type: domain.TaskDeploy, Status: domain.TaskAccepted}
	cfg := WorkerConfig{FailRate: 1, MinDelay: 0, MaxDelay: 0}
	if err := processTask(context.Background(), publisher, task, cfg); err != nil {
		t.Fatalf("process task: %v", err)
	}
	if len(publisher.updates) != 2 {
		t.Fatalf("want 2 progress events, got %d", len(publisher.updates))
	}
	if publisher.updates[0].ID != "task-123:in_progress" || publisher.updates[0].Status != domain.TaskInProgress {
		t.Fatalf("unexpected in_progress event: %+v", publisher.updates[0])
	}
	if publisher.updates[1].ID != "task-123:terminal" || publisher.updates[1].Status != domain.TaskFailed {
		t.Fatalf("unexpected terminal event: %+v", publisher.updates[1])
	}

	// A redelivery of the task uses the same IDs, allowing the consumer to deduplicate it.
	if err := processTask(context.Background(), publisher, task, cfg); err != nil {
		t.Fatalf("process redelivery: %v", err)
	}
	if publisher.updates[2].ID != publisher.updates[0].ID || publisher.updates[3].ID != publisher.updates[1].ID {
		t.Fatal("worker redelivery changed idempotency IDs")
	}
}

func TestWorkerHonorsDelayBounds(t *testing.T) {
	publisher := &recordingPublisher{}
	task := domain.Task{ID: "task-delay", TenantID: "tenant-delay", Type: domain.TaskUpdate, Status: domain.TaskAccepted}
	cfg := WorkerConfig{FailRate: 0, MinDelay: 15 * time.Millisecond, MaxDelay: 15 * time.Millisecond}
	start := time.Now()
	if err := processTask(context.Background(), publisher, task, cfg); err != nil {
		t.Fatalf("process task: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("configured delay was not observed: %s", elapsed)
	}
	if publisher.updates[1].Status != domain.TaskDone {
		t.Fatalf("fail-rate 0 should complete successfully, got %s", publisher.updates[1].Status)
	}
}


func TestWorkerOutcomeIsStableAcrossTaskRedelivery(t *testing.T) {
	publisher := &recordingPublisher{}
	task := domain.Task{ID: "task-stable-outcome", TenantID: "tenant-456", Type: domain.TaskDeploy, Status: domain.TaskAccepted}
	cfg := WorkerConfig{FailRate: 0.5, MinDelay: 0, MaxDelay: 0}

	for attempt := 0; attempt < 10; attempt++ {
		if err := processTask(context.Background(), publisher, task, cfg); err != nil {
			t.Fatalf("process task on attempt %d: %v", attempt, err)
		}
	}
	firstStatus := publisher.updates[1].Status
	for i := 3; i < len(publisher.updates); i += 2 {
		if publisher.updates[i].Status != firstStatus {
			t.Fatalf("terminal outcome changed on redelivery: first=%s retry=%s", firstStatus, publisher.updates[i].Status)
		}
	}
}
