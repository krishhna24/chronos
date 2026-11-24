package pebblestore

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/storetest"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func openTest(t testing.TB, dir string) *Store {
	t.Helper()
	s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		s := openTest(t, t.TempDir())
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

// manifestEveryWrite takes a snapshot manifest after every Write so the
// conformance suite exercises the manifest List path.
type manifestEveryWrite struct{ *Store }

func (m manifestEveryWrite) Write(ctx context.Context, es []store.Entry) error {
	if err := m.Store.Write(ctx, es); err != nil {
		return err
	}
	return m.Snapshot(ctx)
}

func TestConformanceWithManifests(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		s := openTest(t, t.TempDir())
		t.Cleanup(func() { _ = s.Close() })
		return manifestEveryWrite{s}
	})
}

func TestConformanceWithManifestsUsesManifestPath(t *testing.T) {
	var stores []*Store
	storetest.Run(t, func(t *testing.T) store.Store {
		s := openTest(t, t.TempDir())
		t.Cleanup(func() { _ = s.Close() })
		stores = append(stores, s)
		return manifestEveryWrite{s}
	})
	var n int64
	for _, s := range stores {
		n += s.stats.manifest.Load()
		if f := s.stats.fallbacks.Load(); f != 0 {
			t.Errorf("unexpected manifest fallbacks: %d", f)
		}
	}
	if n == 0 {
		t.Fatal("manifest List path never used")
	}
	t.Logf("manifest-served Lists: %d", n)
}
