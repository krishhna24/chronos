package web

import (
	"io/fs"
	"strings"
	"testing"
)

func TestFSServesIndex(t *testing.T) {
	b, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatalf("index.html: %v", err)
	}
	if !strings.Contains(string(b), "<html") {
		t.Fatalf("index.html does not look like HTML: %.80q", b)
	}
	if !Built() && !strings.Contains(string(b), "make web") {
		t.Fatalf("placeholder should explain how to build the dashboard")
	}
}
