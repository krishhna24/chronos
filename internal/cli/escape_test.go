package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/api"
)

func TestEscapeControls(t *testing.T) {
	for in, want := range map[string]string{
		"plain text":          "plain text",
		"ünïcödé ✓":           "ünïcödé ✓",
		"a\x1b[31mred":        "a^[[31mred",
		"title\x1b]0;x\a":     `title^[]0;x\x07`,
		"tab\there":           `tab\there`,
		"line\nforged row":    `line\nforged row`,
		"cr\rover":            `cr\rover`,
		"del\x7f c1\u009bcsi": `del\x7f c1\u009bcsi`,
		"bad\xffutf8":         `bad\xffutf8`,
	} {
		if got := safeCell(in); got != want {
			t.Errorf("safeCell(%q) = %q, want %q", in, got, want)
		}
	}
	if got := safeText("two\nlines\x1b"); got != "two\nlines^[" {
		t.Errorf("safeText kept/escaped wrongly: %q", got)
	}
}

// Regression for the review finding: Event message/reason written by any
// cluster user reached the terminal with raw escape sequences.
func TestTableEscapesRecordedStrings(t *testing.T) {
	rt := api.Resource{ID: "v1/events", Version: "v1", Resource: "events", Kind: "Event", Namespaced: true}
	obj := []byte(`{"apiVersion":"v1","kind":"Event","metadata":{"name":"e1","namespace":"ns"},` +
		`"involvedObject":{"kind":"Pod","name":"p"},"reason":"x\u001b[31m","type":"Normal",` +
		`"message":"hello\u001b]0;owned\u0007\u001b[2J\u001b[1A\rfake\nline\tcol"}`)
	items := []api.ObjectState{{Key: api.ObjectKey{Resource: rt.ID, Namespace: "ns", Name: "e1"}, Existence: api.ExistencePresent, Object: obj,
		Version: &api.Version{ObservedAt: time.Now(), Type: "Added", Origin: "watch"}}}
	var buf bytes.Buffer
	if err := printTable(&buf, rt, items, tableOptions{at: time.Now()}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, bad := range []string{"\x1b", "\a", "\r"} {
		if strings.Contains(out, bad) {
			t.Errorf("table output contains %q: %q", bad, out)
		}
	}
	if n := strings.Count(out, "\n"); n != 2 {
		t.Errorf("table has %d lines, want 2 (header + one row): %q", n, out)
	}
}
