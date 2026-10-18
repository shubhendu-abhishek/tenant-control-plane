package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/domain"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const TaskTopic = "provisioning.tasks"

type Store struct{ db *sql.DB }

type AppError struct {
	Code    string
	Message string
}

func (e *AppError) Error() string { return e.Code + ": " + e.Message }

func appError(code, message string) error { return &AppError{Code: code, Message: message} }

func ErrorCode(err error) string {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr.Code
	}
	return ""
}

type OutboxEvent struct {
	ID       int64
	EventID  string
	Topic    string
	Payload  []byte
	Attempts int
}

func Open(databaseURL string) (*Store, error) {
	// The blank import registers the pgx database/sql driver.
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error                   { return s.db.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	contents, err := migrationFiles.ReadFile("migrations/001_init.sql")
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}
	// Execute statements individually so this works with pgx's prepared-query
	// protocol as well as database/sql implementations that reject multi-statements.
	for _, statement := range strings.Split(string(contents), ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("run migration statement %q: %w", statement, err)
		}
	}
	return nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func databaseTime() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

func uniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "tenants_slug_key"
}

func (s *Store) CreateTenant(ctx context.Context, slug, name string) (domain.Tenant, domain.Task, error) {
	var tenant domain.Tenant
	var task domain.Task
	tenantID, err := newID()
	if err != nil {
		return tenant, task, err
	}
	taskID, err := newID()
	if err != nil {
		return tenant, task, err
	}
	now := databaseTime()
	tenant = domain.Tenant{ID: tenantID, Slug: slug, Name: name, Status: domain.TenantProvisioning, Version: 1, CreatedAt: domain.NewTimestamp(now), UpdatedAt: domain.NewTimestamp(now)}
	task = domain.Task{ID: taskID, Type: domain.TaskDeploy, TenantID: tenantID, Status: domain.TaskAccepted, CreatedAt: domain.NewTimestamp(now), UpdatedAt: domain.NewTimestamp(now)}
	payload, err := json.Marshal(task)
	if err != nil {
		return tenant, task, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return tenant, task, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO tenants(id,slug,name,status,version,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, tenant.ID, tenant.Slug, tenant.Name, tenant.Status, tenant.Version, now, now)
	if err != nil {
		if uniqueViolation(err) {
			return tenant, task, appError("tenant_already_exists", "a tenant with this slug already exists")
		}
		return tenant, task, fmt.Errorf("insert tenant: %w", err)
	}
	if err := insertTaskAndOutbox(ctx, tx, task, payload); err != nil {
		return tenant, task, err
	}
	if err := tx.Commit(); err != nil {
		return tenant, task, fmt.Errorf("commit tenant creation: %w", err)
	}
	return tenant, task, nil
}

func insertTaskAndOutbox(ctx context.Context, tx *sql.Tx, task domain.Task, payload []byte) error {
	now := task.CreatedAt.Time()
	if _, err := tx.ExecContext(ctx, `INSERT INTO tasks(id,type,tenant_id,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6)`, task.ID, task.Type, task.TenantID, task.Status, now, now); err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox(event_id,topic,payload) VALUES($1,$2,$3::jsonb)`, task.ID, TaskTopic, string(payload)); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func (s *Store) PatchTenant(ctx context.Context, id string, expectedVersion int64, name string) (domain.Tenant, domain.Task, error) {
	var tenant domain.Tenant
	var task domain.Task
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return tenant, task, err
	}
	defer tx.Rollback()
	current, err := getTenantTx(ctx, tx, id, true)
	if err != nil {
		return tenant, task, err
	}
	if current.Version != expectedVersion {
		return tenant, task, appError("tenant_version_conflict", fmt.Sprintf("expected version %d but current version is %d", expectedVersion, current.Version))
	}
	if current.Status != domain.TenantActive {
		return tenant, task, appError("tenant_update_not_allowed", fmt.Sprintf("tenant cannot be updated while status is %q", current.Status))
	}
	taskID, err := newID()
	if err != nil {
		return tenant, task, err
	}
	now := databaseTime()
	if _, err := tx.ExecContext(ctx, `UPDATE tenants SET name=$1,status=$2,version=version+1,updated_at=$3 WHERE id=$4 AND version=$5`, name, domain.TenantUpdating, now, id, expectedVersion); err != nil {
		return tenant, task, fmt.Errorf("update tenant: %w", err)
	}
	tenant = current
	tenant.Name = name
	tenant.Status = domain.TenantUpdating
	tenant.Version++
	tenant.UpdatedAt = domain.NewTimestamp(now)
	task = domain.Task{ID: taskID, Type: domain.TaskUpdate, TenantID: id, Status: domain.TaskAccepted, CreatedAt: domain.NewTimestamp(now), UpdatedAt: domain.NewTimestamp(now)}
	payload, err := json.Marshal(task)
	if err != nil {
		return tenant, task, err
	}
	if err := insertTaskAndOutbox(ctx, tx, task, payload); err != nil {
		return tenant, task, err
	}
	if err := tx.Commit(); err != nil {
		return tenant, task, fmt.Errorf("commit tenant update: %w", err)
	}
	return tenant, task, nil
}

func (s *Store) DeleteTenant(ctx context.Context, id string) (domain.Tenant, domain.Task, error) {
	var tenant domain.Tenant
	var task domain.Task
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return tenant, task, err
	}
	defer tx.Rollback()
	current, err := getTenantTx(ctx, tx, id, true)
	if err != nil {
		return tenant, task, err
	}
	if current.Status != domain.TenantActive && current.Status != domain.TenantFailed {
		return tenant, task, appError("tenant_update_not_allowed", fmt.Sprintf("tenant cannot be destroyed while status is %q", current.Status))
	}
	taskID, err := newID()
	if err != nil {
		return tenant, task, err
	}
	now := databaseTime()
	if _, err := tx.ExecContext(ctx, `UPDATE tenants SET status=$1,version=version+1,updated_at=$2 WHERE id=$3 AND version=$4`, domain.TenantDestroying, now, id, current.Version); err != nil {
		return tenant, task, fmt.Errorf("mark tenant destroying: %w", err)
	}
	tenant = current
	tenant.Status = domain.TenantDestroying
	tenant.Version++
	tenant.UpdatedAt = domain.NewTimestamp(now)
	task = domain.Task{ID: taskID, Type: domain.TaskDestroy, TenantID: id, Status: domain.TaskAccepted, CreatedAt: domain.NewTimestamp(now), UpdatedAt: domain.NewTimestamp(now)}
	payload, err := json.Marshal(task)
	if err != nil {
		return tenant, task, err
	}
	if err := insertTaskAndOutbox(ctx, tx, task, payload); err != nil {
		return tenant, task, err
	}
	if err := tx.Commit(); err != nil {
		return tenant, task, fmt.Errorf("commit tenant delete: %w", err)
	}
	return tenant, task, nil
}

func getTenantTx(ctx context.Context, tx *sql.Tx, id string, forUpdate bool) (domain.Tenant, error) {
	query := `SELECT id,slug,name,status,version,created_at,updated_at FROM tenants WHERE id=$1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	tenant, err := scanTenant(tx.QueryRowContext(ctx, query, id))
	if errors.Is(err, sql.ErrNoRows) {
		return tenant, appError("tenant_not_found", "tenant was not found")
	}
	return tenant, err
}

type scanner interface{ Scan(dest ...any) error }

func scanTenant(row scanner) (domain.Tenant, error) {
	var t domain.Tenant
	var createdAt, updatedAt time.Time
	err := row.Scan(&t.ID, &t.Slug, &t.Name, &t.Status, &t.Version, &createdAt, &updatedAt)
	t.CreatedAt, t.UpdatedAt = domain.NewTimestamp(createdAt), domain.NewTimestamp(updatedAt)
	return t, err
}
func scanTask(row scanner) (domain.Task, error) {
	var t domain.Task
	var createdAt, updatedAt time.Time
	err := row.Scan(&t.ID, &t.Type, &t.TenantID, &t.Status, &createdAt, &updatedAt)
	t.CreatedAt, t.UpdatedAt = domain.NewTimestamp(createdAt), domain.NewTimestamp(updatedAt)
	return t, err
}

func (s *Store) GetTenant(ctx context.Context, id string) (domain.Tenant, error) {
	tenant, err := scanTenant(s.db.QueryRowContext(ctx, `SELECT id,slug,name,status,version,created_at,updated_at FROM tenants WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return tenant, appError("tenant_not_found", "tenant was not found")
	}
	return tenant, err
}

func (s *Store) ListTenants(ctx context.Context, status string, limit, offset int) ([]domain.Tenant, error) {
	query := `SELECT id,slug,name,status,version,created_at,updated_at FROM tenants`
	args := []any{}
	if status != "" {
		query += ` WHERE status=$1`
		args = append(args, status)
	}
	query += fmt.Sprintf(` ORDER BY created_at DESC,id LIMIT $%d OFFSET $%d`, len(args)+1, len(args)+2)
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.Tenant, 0)
	for rows.Next() {
		item, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetTask(ctx context.Context, id string) (domain.Task, error) {
	task, err := scanTask(s.db.QueryRowContext(ctx, `SELECT id,type,tenant_id,status,created_at,updated_at FROM tasks WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return task, appError("task_not_found", "task was not found")
	}
	return task, err
}

func (s *Store) ListTasks(ctx context.Context, tenantID, status string, limit, offset int) ([]domain.Task, error) {
	conditions, args := []string{}, []any{}
	if tenantID != "" {
		args = append(args, tenantID)
		conditions = append(conditions, fmt.Sprintf("tenant_id=$%d", len(args)))
	}
	if status != "" {
		args = append(args, status)
		conditions = append(conditions, fmt.Sprintf("status=$%d", len(args)))
	}
	query := `SELECT id,type,tenant_id,status,created_at,updated_at FROM tasks`
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += fmt.Sprintf(` ORDER BY created_at DESC,id LIMIT $%d OFFSET $%d`, len(args)+1, len(args)+2)
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domain.Task, 0)
	for rows.Next() {
		item, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ApplyProgress applies a worker update in one transaction. Duplicate update IDs,
// updates to terminal tasks, and out-of-order transitions are safe no-ops.
func (s *Store) ApplyProgress(ctx context.Context, update domain.ProgressUpdate) error {
	if update.ID == "" || update.TaskID == "" {
		return appError("invalid_progress_update", "id and task_id are required")
	}
	if update.Status != domain.TaskInProgress && update.Status != domain.TaskDone && update.Status != domain.TaskFailed {
		return appError("invalid_progress_update", "status must be in_progress, done, or failed")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	task, err := scanTask(tx.QueryRowContext(ctx, `SELECT id,type,tenant_id,status,created_at,updated_at FROM tasks WHERE id=$1 FOR UPDATE`, update.TaskID))
	if errors.Is(err, sql.ErrNoRows) {
		return appError("task_not_found", "task was not found")
	}
	if err != nil {
		return fmt.Errorf("read task for progress update: %w", err)
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO processed_updates(update_id,task_id,status) VALUES($1,$2,$3) ON CONFLICT(update_id) DO NOTHING`, update.ID, update.TaskID, update.Status)
	if err != nil {
		return fmt.Errorf("record progress idempotency key: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return tx.Commit()
	}
	if task.Status == domain.TaskDone || task.Status == domain.TaskFailed {
		return tx.Commit()
	}
	now := databaseTime()
	if update.Status == domain.TaskInProgress {
		if task.Status == domain.TaskAccepted {
			if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status=$1,updated_at=$2 WHERE id=$3 AND status=$4`, domain.TaskInProgress, now, task.ID, domain.TaskAccepted); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	if task.Status != domain.TaskAccepted && task.Status != domain.TaskInProgress {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status=$1,updated_at=$2 WHERE id=$3 AND status IN ($4,$5)`, update.Status, now, task.ID, domain.TaskAccepted, domain.TaskInProgress); err != nil {
		return err
	}
	expectedTenantStatus, nextTenantStatus := tenantTransition(task.Type, update.Status)
	if expectedTenantStatus != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE tenants SET status=$1,version=version+1,updated_at=$2 WHERE id=$3 AND status=$4`, nextTenantStatus, now, task.TenantID, expectedTenantStatus); err != nil {
			return fmt.Errorf("apply tenant terminal state: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit progress update: %w", err)
	}
	return nil
}

func tenantTransition(taskType domain.TaskType, taskStatus domain.TaskStatus) (domain.TenantStatus, domain.TenantStatus) {
	if taskStatus != domain.TaskDone && taskStatus != domain.TaskFailed {
		return "", ""
	}
	var expected, success domain.TenantStatus
	switch taskType {
	case domain.TaskDeploy:
		expected, success = domain.TenantProvisioning, domain.TenantActive
	case domain.TaskUpdate:
		expected, success = domain.TenantUpdating, domain.TenantActive
	case domain.TaskDestroy:
		expected, success = domain.TenantDestroying, domain.TenantDestroyed
	default:
		return "", ""
	}
	if taskStatus == domain.TaskFailed {
		return expected, domain.TenantFailed
	}
	return expected, success
}

func (s *Store) ClaimOutbox(ctx context.Context) (*OutboxEvent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var event OutboxEvent
	err = tx.QueryRowContext(ctx, `SELECT id,event_id,topic,payload,attempts FROM outbox WHERE published_at IS NULL AND next_attempt_at <= now() ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&event.ID, &event.EventID, &event.Topic, &event.Payload, &event.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	event.Attempts++
	if _, err := tx.ExecContext(ctx, `UPDATE outbox SET attempts=$1,next_attempt_at=now()+interval '30 seconds' WHERE id=$2`, event.Attempts, event.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &event, nil
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbox SET published_at=now(),last_error=NULL WHERE id=$1`, id)
	return err
}

func (s *Store) MarkOutboxFailed(ctx context.Context, id int64, publishErr error, retryAfter time.Duration) error {
	if retryAfter < time.Second {
		retryAfter = time.Second
	}
	_, err := s.db.ExecContext(ctx, `UPDATE outbox SET last_error=$2,next_attempt_at=now()+($3 * interval '1 second') WHERE id=$1`, id, truncateError(publishErr.Error(), 1000), retryAfter.Seconds())
	return err
}

func truncateError(message string, max int) string {
	if len(message) > max {
		return message[:max]
	}
	return message
}
