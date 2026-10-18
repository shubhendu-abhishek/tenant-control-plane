package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/shubhendu-abhishek/tenant-control-plane/internal/domain"
	"github.com/shubhendu-abhishek/tenant-control-plane/internal/store"
)

var slugPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

type Server struct {
	store *store.Store
	mux   *http.ServeMux
}

func New(s *store.Store) *Server {
	server := &Server{store: s, mux: http.NewServeMux()}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler { return s.recoverPanic(s.requestLog(s.mux)) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /readyz", s.ready)
	s.mux.HandleFunc("POST /v1/tenants", s.createTenant)
	s.mux.HandleFunc("GET /v1/tenants", s.listTenants)
	s.mux.HandleFunc("GET /v1/tenants/{id}", s.getTenant)
	s.mux.HandleFunc("PATCH /v1/tenants/{id}", s.patchTenant)
	s.mux.HandleFunc("DELETE /v1/tenants/{id}", s.deleteTenant)
	s.mux.HandleFunc("GET /v1/tasks", s.listTasks)
	s.mux.HandleFunc("GET /v1/tasks/{id}", s.getTask)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "database is not available")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createTenantRequest struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	req.Slug, req.Name = strings.TrimSpace(req.Slug), strings.TrimSpace(req.Name)
	if !slugPattern.MatchString(req.Slug) {
		writeError(w, http.StatusBadRequest, "validation_error", "slug must be 3-28 chars and match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "name must not be empty")
		return
	}
	tenant, task, err := s.store.CreateTenant(r.Context(), req.Slug, req.Name)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, domain.TenantTaskResponse{Tenant: tenant, Task: task})
}

func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "validation_error", err.Error())
		return
	}
	status := r.URL.Query().Get("status")
	if status != "" && !validTenantStatus(domain.TenantStatus(status)) {
		writeError(w, http.StatusBadRequest, "validation_error", "unknown tenant status")
		return
	}
	items, err := s.store.ListTenants(r.Context(), status, limit, offset)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, domain.TenantListResponse{Items: items, Limit: limit, Offset: offset})
}

func (s *Server) getTenant(w http.ResponseWriter, r *http.Request) {
	tenant, err := s.store.GetTenant(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenant)
}

type patchTenantRequest struct {
	Version int64   `json:"version"`
	Name    *string `json:"name"`
}

func (s *Server) patchTenant(w http.ResponseWriter, r *http.Request) {
	var req patchTenantRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if req.Version < 1 {
		writeError(w, http.StatusBadRequest, "validation_error", "version must be a positive integer")
		return
	}
	if req.Name == nil {
		writeError(w, http.StatusBadRequest, "validation_error", "at least one mutable field is required; currently supported field: name")
		return
	}
	name := strings.TrimSpace(*req.Name)
	if name == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "name must not be empty")
		return
	}
	tenant, task, err := s.store.PatchTenant(r.Context(), r.PathValue("id"), req.Version, name)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, domain.TenantTaskResponse{Tenant: tenant, Task: task})
}

func (s *Server) deleteTenant(w http.ResponseWriter, r *http.Request) {
	tenant, task, err := s.store.DeleteTenant(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, domain.TenantTaskResponse{Tenant: tenant, Task: task})
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := pagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "validation_error", err.Error())
		return
	}
	status := r.URL.Query().Get("status")
	if status != "" && !validTaskStatus(domain.TaskStatus(status)) {
		writeError(w, http.StatusBadRequest, "validation_error", "unknown task status")
		return
	}
	items, err := s.store.ListTasks(r.Context(), r.URL.Query().Get("tenant_id"), status, limit, offset)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, domain.TaskListResponse{Items: items, Limit: limit, Offset: offset})
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	task, err := s.store.GetTask(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	switch store.ErrorCode(err) {
	case "tenant_not_found":
		writeError(w, http.StatusNotFound, "tenant_not_found", "tenant was not found")
	case "tenant_already_exists":
		writeError(w, http.StatusConflict, "tenant_already_exists", "a tenant with this slug already exists")
	case "tenant_update_not_allowed":
		writeError(w, http.StatusConflict, "tenant_update_not_allowed", errorMessage(err))
	case "tenant_version_conflict":
		writeError(w, http.StatusConflict, "tenant_version_conflict", errorMessage(err))
	case "task_not_found":
		writeError(w, http.StatusNotFound, "task_not_found", "task was not found")
	case "invalid_progress_update":
		writeError(w, http.StatusBadRequest, "validation_error", errorMessage(err))
	default:
		slog.Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "an unexpected error occurred")
	}
}

func errorMessage(err error) string {
	var appErr *store.AppError
	if errors.As(err, &appErr) {
		return appErr.Message
	}
	return err.Error()
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("request body must contain a single JSON value")
	}
	return nil
}

func pagination(r *http.Request) (int, int, error) {
	limit, offset := 50, 0
	var err error
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, errors.New("limit must be an integer")
		}
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		offset, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, errors.New("offset must be an integer")
		}
	}
	if limit < 1 || limit > 100 {
		return 0, 0, errors.New("limit must be between 1 and 100")
	}
	if offset < 0 {
		return 0, 0, errors.New("offset must be zero or greater")
	}
	return limit, offset, nil
}

func validTenantStatus(status domain.TenantStatus) bool {
	switch status {
	case domain.TenantProvisioning, domain.TenantActive, domain.TenantUpdating, domain.TenantDestroying, domain.TenantDestroyed, domain.TenantFailed:
		return true
	default:
		return false
	}
}
func validTaskStatus(status domain.TaskStatus) bool {
	switch status {
	case domain.TaskAccepted, domain.TaskInProgress, domain.TaskDone, domain.TaskFailed:
		return true
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("write response", "error", err)
	}
}
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, domain.ErrorResponse{Error: domain.ErrorBody{Code: code, Message: message}})
}

func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "status", wrapped.status, "duration_ms", time.Since(start).Milliseconds())
	})
}
func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("panic recovered", "panic", recovered)
				writeError(w, http.StatusInternalServerError, "internal_error", "an unexpected error occurred")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
