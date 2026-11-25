package pebblestore

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestRateLimitedLog(t *testing.T) {
	var buf bytes.Buffer
	now := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	r := newRateLimitedLog(slog.New(slog.NewTextHandler(&buf, nil)), "bg", 10*time.Second, func() time.Time { return now })
	for range 500 {
		r.report(errors.New("no space left on device"))
		now = now.Add(100 * time.Millisecond)
	}
	// 50 s of errors every 100 ms: one line at 0 s, then one per 10 s.
	lines := strings.Count(buf.String(), "msg=bg")
	if lines != 5 {
		t.Fatalf("logged %d lines, want 5:\n%s", lines, buf.String())
	}
	if !strings.Contains(buf.String(), "suppressed=99") {
		t.Errorf("suppressed count missing:\n%s", buf.String())
	}
}
