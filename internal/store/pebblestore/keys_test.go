package pebblestore

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

func TestComponentRoundTripAndOrder(t *testing.T) {
	strs := []string{"", "a", "ab", "a\x00", "a\x00b", "a\x01", "prod", "prod-2", "prod\xff", "\x00", "\x00\x00", "\xff"}
	for _, s := range strs {
		enc := appendComponent(nil, s)
		got, rest, err := decodeComponent(append(enc, 'x'))
		if err != nil || got != s || string(rest) != "x" {
			t.Errorf("round trip %q: got %q rest %q err %v", s, got, rest, err)
		}
	}
	for _, a := range strs {
		for _, b := range strs {
			ea, eb := appendComponent(nil, a), appendComponent(nil, b)
			if want, got := cmpStr(a, b), bytes.Compare(ea, eb); want != got {
				t.Errorf("order %q vs %q: raw %d encoded %d", a, b, want, got)
			}
			if a != b && bytes.HasPrefix(eb, ea) {
				t.Errorf("encoded %q is a prefix of encoded %q", a, b)
			}
		}
	}
}

func cmpStr(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func TestTimeEncodingOrderAndRange(t *testing.T) {
	r := newRand(1)
	times := []time.Time{{}, time.Unix(0, 0), time.Unix(-1, 999999999), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)}
	for range 200 {
		times = append(times, time.Unix(r.Int64N(1<<40)-(1<<39), r.Int64N(1e9)))
	}
	for _, a := range times {
		dec, err := decodeTime(appendTime(nil, a))
		if err != nil || !dec.Equal(a) {
			t.Fatalf("round trip %s: %s %v", a, dec, err)
		}
		for _, b := range times {
			want := a.Compare(b)
			if got := bytes.Compare(appendTime(nil, a), appendTime(nil, b)); got != want {
				t.Fatalf("order %s vs %s: %d want %d", a, b, got, want)
			}
		}
	}
}

func TestVersionKeyPrefixIsolation(t *testing.T) {
	at := time.Unix(100, 0)
	k := func(ns, name string) model.ObjectKey {
		return model.ObjectKey{Resource: "v1/pods", Namespace: ns, Name: name}
	}
	if bytes.HasPrefix(versionKey(k("prod-2", "a"), at), versionNamespacePrefix("v1/pods", "prod")) {
		t.Error("prod-2 key under prod prefix")
	}
	if bytes.HasPrefix(versionKey(k("prod", "ab"), at), objectPrefix(k("prod", "a"))) {
		t.Error("ab key under a prefix")
	}
	if bytes.HasPrefix(versionKey(model.ObjectKey{Resource: "v1/pods2", Name: "x"}, at), versionResourcePrefix("v1/pods")) {
		t.Error("pods2 under pods prefix")
	}
	got, gt, err := parseVersionKey(versionKey(k("ns", "n"), at))
	if err != nil || got != k("ns", "n") || !gt.Equal(at) {
		t.Errorf("parse: %v %s %v", got, gt, err)
	}
}

func TestSealDetectsCorruption(t *testing.T) {
	key := []byte("Vkey")
	v := seal(key, []byte("payload"))
	if p, err := unseal(key, v); err != nil || string(p) != "payload" {
		t.Fatalf("unseal: %q %v", p, err)
	}
	for i := range v {
		bad := bytes.Clone(v)
		bad[i] ^= 0x40
		if _, err := unseal(key, bad); !errors.Is(err, store.ErrCorrupt) {
			t.Errorf("flip byte %d: err=%v", i, err)
		}
	}
	if _, err := unseal([]byte("Vother"), v); !errors.Is(err, store.ErrCorrupt) {
		t.Errorf("value under wrong key accepted: %v", err)
	}
	if _, err := unseal(key, v[:3]); !errors.Is(err, store.ErrCorrupt) {
		t.Errorf("truncated value accepted: %v", err)
	}
}

// newRand returns a deterministic generator for reproducible tests.
func newRand(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)) //nolint:gosec // reproducible test data, not security
}

func TestListStartSeeksPastAfterKey(t *testing.T) {
	at := time.Unix(100, 0)
	after := podKey("ab", "web-0")
	start := listStart(store.ListOptions{Resource: "v1/pods", After: &after})
	for _, k := range []model.ObjectKey{after, podKey("ab", "web"), podKey("a-", "z"), podKey("a", "zz")} {
		if bytes.Compare(versionKey(k, at), start) >= 0 {
			t.Errorf("%s at or before After but sorts after the seek key", k)
		}
	}
	for _, k := range []model.ObjectKey{podKey("ab", "web-00"), podKey("ab", "web-0\x01"), podKey("b", ""), podKey("b", "a"), podKey("abc", "a")} {
		if bytes.Compare(versionKey(k, time.Time{}), start) < 0 {
			t.Errorf("%s after After but sorts before the seek key", k)
		}
	}
}
