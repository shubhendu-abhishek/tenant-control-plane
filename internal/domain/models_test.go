package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestTimestampRoundTripUsesUTCMilliseconds(t *testing.T) {
	input := `{"id":"task:done","task_id":"task","status":"done","occurred_at":"2026-10-18T12:34:56.123987+02:00"}`
	var update ProgressUpdate
	if err := json.Unmarshal([]byte(input), &update); err != nil {
		t.Fatalf("unmarshal progress update: %v", err)
	}
	encoded, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("marshal progress update: %v", err)
	}
	if !strings.Contains(string(encoded), ` "occurred_at":"2026-10-18T10:34:56.123Z" `) {
		t.Fatalf("timestamp must be UTC with millisecond precision, got %s", encoded)
	}
	if got := time.Time(update.OccurredAt).Nanosecond(); got != 123000000 {
		t.Fatalf("timestamp must be truncated to milliseconds, got nanoseconds=%d", got)
	}
}
