package domain

import (
	"encoding/json"
	"time"
)

type TenantStatus string

const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantUpdating     TenantStatus = "updating"
	TenantDestroying   TenantStatus = "destroying"
	TenantDestroyed    TenantStatus = "destroyed"
	TenantFailed       TenantStatus = "failed"
)

type TaskType string

const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

type TaskStatus string

const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

// Timestamp always serializes as UTC with exactly millisecond precision.
type Timestamp time.Time

func NewTimestamp(t time.Time) Timestamp { return Timestamp(t.UTC().Truncate(time.Millisecond)) }

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format("2006-01-02T15:04:05.000Z"))
}

func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var parsed time.Time
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	*t = NewTimestamp(parsed)
	return nil
}

func (t Timestamp) Time() time.Time { return time.Time(t).UTC() }

type Tenant struct {
	ID        string       `json:"id"`
	Slug      string       `json:"slug"`
	Name      string       `json:"name"`
	Status    TenantStatus `json:"status"`
	Version   int64        `json:"version"`
	CreatedAt Timestamp    `json:"created_at"`
	UpdatedAt Timestamp    `json:"updated_at"`
}

type Task struct {
	ID        string     `json:"id"`
	Type      TaskType   `json:"type"`
	TenantID  string     `json:"tenant_id"`
	Status    TaskStatus `json:"status"`
	CreatedAt Timestamp  `json:"created_at"`
	UpdatedAt Timestamp  `json:"updated_at"`
}

// ProgressUpdate is the worker-to-control-plane contract. ID is a stable
// idempotency key (for example, "<task-id>:done").
type ProgressUpdate struct {
	ID         string     `json:"id"`
	TaskID     string     `json:"task_id"`
	Status     TaskStatus `json:"status"`
	OccurredAt Timestamp  `json:"occurred_at"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type TenantTaskResponse struct {
	Tenant Tenant `json:"tenant"`
	Task   Task   `json:"task"`
}

type TenantListResponse struct {
	Items  []Tenant `json:"items"`
	Limit  int      `json:"limit"`
	Offset int      `json:"offset"`
}

type TaskListResponse struct {
	Items  []Task `json:"items"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}
