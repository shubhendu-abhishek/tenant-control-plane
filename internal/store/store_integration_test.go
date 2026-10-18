package store

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubhendu-abhishek/tenant-control-plane/internal/domain"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		t.Skip("set TEST_DATABASE_URL or DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var st *Store
	var err error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		st, err = Open(url)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		st.Close()
		t.Fatalf("migrate database: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func uniqueSlug() string { return "t" + strconv.FormatInt(time.Now().UnixNano(), 36) }

func createIntegrationTenant(t *testing.T, st *Store) (domain.Tenant, domain.Task) {
	t.Helper()
	tenant, task, err := st.CreateTenant(context.Background(), uniqueSlug(), "Example tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	return tenant, task
}

func apply(t *testing.T, st *Store, update domain.ProgressUpdate) {
	t.Helper()
	if err := st.ApplyProgress(context.Background(), update); err != nil {
		t.Fatalf("apply progress update %+v: %v", update, err)
	}
}

func TestLifecycleIdempotencyAndOutOfOrderUpdates(t *testing.T) {
	st := integrationStore(t)
	tenant, task := createIntegrationTenant(t, st)
	var outboxCount int
	if err := st.db.QueryRow(`SELECT count(*) FROM outbox WHERE event_id=$1`, task.ID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("want exactly one outbox row for task, got %d", outboxCount)
	}

	apply(t, st, domain.ProgressUpdate{ID: task.ID + ":in_progress", TaskID: task.ID, Status: domain.TaskInProgress})
	apply(t, st, domain.ProgressUpdate{ID: task.ID + ":done", TaskID: task.ID, Status: domain.TaskDone})
	// Same id delivered again is ignored, even if the body was changed.
	apply(t, st, domain.ProgressUpdate{ID: task.ID + ":done", TaskID: task.ID, Status: domain.TaskFailed})
	// A distinct stale event arriving after the terminal event also cannot regress state.
	apply(t, st, domain.ProgressUpdate{ID: task.ID + ":late-progress", TaskID: task.ID, Status: domain.TaskInProgress})

	gotTenant, err := st.GetTenant(context.Background(), tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	gotTask, err := st.GetTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotTenant.Status != domain.TenantActive || gotTenant.Version != 2 {
		t.Fatalf("tenant after deploy: status=%s version=%d", gotTenant.Status, gotTenant.Version)
	}
	if gotTask.Status != domain.TaskDone {
		t.Fatalf("task regressed from terminal state: %s", gotTask.Status)
	}
}

func TestConcurrentPatchWithSameVersionOnlyOneSucceeds(t *testing.T) {
	st := integrationStore(t)
	tenant, deployTask := createIntegrationTenant(t, st)
	apply(t, st, domain.ProgressUpdate{ID: deployTask.ID + ":done", TaskID: deployTask.ID, Status: domain.TaskDone})
	tenant, err := st.GetTenant(context.Background(), tenant.ID)
	if err != nil {
		t.Fatal(err)
	}

	var successes atomic.Int32
	var conflicts atomic.Int32
	var unexpected atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			_, _, err := st.PatchTenant(context.Background(), tenant.ID, tenant.Version, name)
			if err == nil {
				successes.Add(1)
				return
			}
			if ErrorCode(err) == "tenant_version_conflict" {
				conflicts.Add(1)
				return
			}
			t.Logf("unexpected patch error: %v", err)
			unexpected.Add(1)
		}(fmt.Sprintf("tenant-%d", i))
	}
	wg.Wait()
	if successes.Load() != 1 || conflicts.Load() != 1 || unexpected.Load() != 0 {
		t.Fatalf("want one success and one version conflict; success=%d conflicts=%d unexpected=%d", successes.Load(), conflicts.Load(), unexpected.Load())
	}
}

func TestDestroyedSlugCannotBeReusedAndFailedTenantCanBeDestroyed(t *testing.T) {
	st := integrationStore(t)
	tenant, deployTask := createIntegrationTenant(t, st)
	apply(t, st, domain.ProgressUpdate{ID: deployTask.ID + ":failed", TaskID: deployTask.ID, Status: domain.TaskFailed})
	got, err := st.GetTenant(context.Background(), tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.TenantFailed || got.Version != 2 {
		t.Fatalf("want failed/version 2, got %s/version %d", got.Status, got.Version)
	}
	destroying, destroyTask, err := st.DeleteTenant(context.Background(), tenant.ID)
	if err != nil {
		t.Fatalf("delete failed tenant: %v", err)
	}
	if destroying.Status != domain.TenantDestroying {
		t.Fatalf("want destroying, got %s", destroying.Status)
	}
	apply(t, st, domain.ProgressUpdate{ID: destroyTask.ID + ":done", TaskID: destroyTask.ID, Status: domain.TaskDone})
	got, err = st.GetTenant(context.Background(), tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.TenantDestroyed {
		t.Fatalf("want destroyed, got %s", got.Status)
	}
	_, _, err = st.CreateTenant(context.Background(), tenant.Slug, "Reused slug")
	if ErrorCode(err) != "tenant_already_exists" {
		t.Fatalf("want tenant_already_exists after destroy, got %v", err)
	}
}

func TestConcurrentCreateSameSlugOnlyOneWins(t *testing.T) {
	st := integrationStore(t)
	slug := uniqueSlug()
	var successes atomic.Int32
	var duplicates atomic.Int32
	var unexpected atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := st.CreateTenant(context.Background(), slug, "Concurrent tenant")
			if err == nil {
				successes.Add(1)
				return
			}
			if ErrorCode(err) == "tenant_already_exists" {
				duplicates.Add(1)
				return
			}
			t.Logf("unexpected create error: %v", err)
			unexpected.Add(1)
		}()
	}
	wg.Wait()
	if successes.Load() != 1 || duplicates.Load() != 1 || unexpected.Load() != 0 {
		t.Fatalf("want one successful create and one duplicate; success=%d duplicates=%d unexpected=%d", successes.Load(), duplicates.Load(), unexpected.Load())
	}
}
