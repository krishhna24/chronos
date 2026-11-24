package pebblestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/krishhna24/chronos/internal/model"
)

// Table prefixes. Every key starts with exactly one of these bytes; see
// doc.go for the full layout.
const (
	tabMeta     byte = 'M' // fixed meta keys
	tabLastRec  byte = 'L' // per-resource latest record time
	tabVersion  byte = 'V' // object versions
	tabIndex    byte = 'T' // time index
	tabCoverage byte = 'C' // coverage events
	tabManifest byte = 'N' // snapshot manifest headers
	tabChunk    byte = 'S' // snapshot manifest chunks
)

var (
	keyFormat    = []byte{tabMeta, 'f', 'o', 'r', 'm', 'a', 't'}
	keyState     = []byte{tabMeta, 's', 't', 'a', 't', 'e'}
	keyHorizon   = []byte{tabMeta, 'h', 'o', 'r', 'i', 'z', 'o', 'n'}
	keyHeartbeat = []byte{tabMeta, 'h', 'e', 'a', 'r', 't', 'b', 'e', 'a', 't'}
)

// timeLen is the size of an encoded timestamp: 8 bytes of seconds since the
// Unix epoch (sign bit flipped so the encoding sorts like the signed value)
// followed by 4 bytes of nanoseconds. It covers the full range of time.Time
// seconds, unlike UnixNano, so the zero time.Time is representable.
const timeLen = 12

func appendTime(b []byte, t time.Time) []byte {
	b = binary.BigEndian.AppendUint64(b, uint64(t.Unix())^(1<<63))
	return binary.BigEndian.AppendUint32(b, uint32(t.Nanosecond()))
}

func decodeTime(b []byte) (time.Time, error) {
	if len(b) != timeLen {
		return time.Time{}, fmt.Errorf("timestamp has %d bytes, want %d", len(b), timeLen)
	}
	sec := int64(binary.BigEndian.Uint64(b) ^ (1 << 63))
	nsec := binary.BigEndian.Uint32(b[8:])
	if nsec >= 1e9 {
		return time.Time{}, fmt.Errorf("timestamp nanoseconds %d out of range", nsec)
	}
	return time.Unix(sec, int64(nsec)).UTC(), nil
}

// normTime strips the monotonic clock reading and the location so that
// in-memory comparisons agree with comparisons of the persisted encoding.
func normTime(t time.Time) time.Time { return t.Round(0).UTC() }

// appendComponent appends s as a self-delimiting, order-preserving key
// component: 0x00 bytes are escaped as 0x00 0xFF and the component ends with
// 0x00 0x01. Byte-wise order of encoded components equals byte-wise order
// of the raw strings, and no encoded component is a prefix of a different
// one, so "prod" never matches "prod-2" and "a" never matches "ab".
func appendComponent(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			b = append(b, 0, 0xFF)
		} else {
			b = append(b, s[i])
		}
	}
	return append(b, 0, 1)
}

var errBadComponent = errors.New("malformed key component")

// decodeComponent reverses appendComponent and returns the remaining bytes.
func decodeComponent(b []byte) (string, []byte, error) {
	var sb strings.Builder
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] != 0 {
			continue
		}
		if i+1 >= len(b) {
			return "", nil, errBadComponent
		}
		switch b[i+1] {
		case 1:
			sb.Write(b[start:i])
			return sb.String(), b[i+2:], nil
		case 0xFF:
			sb.Write(b[start:i])
			sb.WriteByte(0)
			i++
			start = i + 1
		default:
			return "", nil, errBadComponent
		}
	}
	return "", nil, errBadComponent
}

// prefixEnd returns the smallest key greater than every key with prefix p.
func prefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // p is all 0xFF: no upper bound
}

// atOrBefore returns an exclusive upper bound that admits every key of the
// form prefix+time with time <= t. Keys under such prefixes are exactly
// len(prefix)+timeLen bytes long, so prefix+time(t)+0xFF sorts after
// prefix+time(t) and before prefix+time(t') for any t' > t.
func atOrBefore(prefix []byte, t time.Time) []byte {
	b := appendTime(append([]byte(nil), prefix...), t)
	return append(b, 0xFF)
}

func versionResourcePrefix(resource string) []byte {
	return appendComponent([]byte{tabVersion}, resource)
}

func versionNamespacePrefix(resource, namespace string) []byte {
	return appendComponent(versionResourcePrefix(resource), namespace)
}

func objectPrefix(k model.ObjectKey) []byte {
	return appendComponent(versionNamespacePrefix(k.Resource, k.Namespace), k.Name)
}

func versionKey(k model.ObjectKey, t time.Time) []byte {
	return appendTime(objectPrefix(k), t)
}

// parseVersionKey splits a version key into its object key and time.
func parseVersionKey(key []byte) (model.ObjectKey, time.Time, error) {
	var k model.ObjectKey
	if len(key) < 1+timeLen || key[0] != tabVersion {
		return k, time.Time{}, errors.New("not a version key")
	}
	rest := key[1:]
	var err error
	if k.Resource, rest, err = decodeComponent(rest); err != nil {
		return k, time.Time{}, err
	}
	if k.Namespace, rest, err = decodeComponent(rest); err != nil {
		return k, time.Time{}, err
	}
	if k.Name, rest, err = decodeComponent(rest); err != nil {
		return k, time.Time{}, err
	}
	t, err := decodeTime(rest)
	return k, t, err
}

func indexKey(t time.Time) []byte { return appendTime([]byte{tabIndex}, t) }

func coveragePrefix(resource string) []byte {
	return appendComponent([]byte{tabCoverage}, resource)
}

func coverageKey(resource string, seq uint64) []byte {
	return binary.BigEndian.AppendUint64(coveragePrefix(resource), seq)
}

func parseCoverageKey(key []byte) (string, uint64, error) {
	if len(key) < 1 || key[0] != tabCoverage {
		return "", 0, errors.New("not a coverage key")
	}
	res, rest, err := decodeComponent(key[1:])
	if err != nil {
		return "", 0, err
	}
	if len(rest) != 8 {
		return "", 0, fmt.Errorf("coverage seq has %d bytes", len(rest))
	}
	return res, binary.BigEndian.Uint64(rest), nil
}

func lastRecKey(resource string) []byte {
	return appendComponent([]byte{tabLastRec}, resource)
}

func manifestKey(s time.Time) []byte { return appendTime([]byte{tabManifest}, s) }

func chunkPrefix(s time.Time) []byte { return appendTime([]byte{tabChunk}, s) }

func chunkKey(s time.Time, resource, namespace string) []byte {
	return appendComponent(appendComponent(chunkPrefix(s), resource), namespace)
}

// describeKey renders a key for error messages and logs.
func describeKey(key []byte) string {
	if len(key) == 0 {
		return "<empty>"
	}
	switch key[0] {
	case tabVersion:
		if k, t, err := parseVersionKey(key); err == nil {
			return fmt.Sprintf("version[%s@%s]", k, t.Format(time.RFC3339Nano))
		}
	case tabIndex:
		if t, err := decodeTime(key[1:]); err == nil {
			return fmt.Sprintf("index[%s]", t.Format(time.RFC3339Nano))
		}
	case tabCoverage:
		if r, seq, err := parseCoverageKey(key); err == nil {
			return fmt.Sprintf("coverage[%s#%d]", r, seq)
		}
	case tabManifest:
		if t, err := decodeTime(key[1:]); err == nil {
			return fmt.Sprintf("manifest[%s]", t.Format(time.RFC3339Nano))
		}
	case tabChunk:
		if len(key) > 1+timeLen {
			if t, err := decodeTime(key[1 : 1+timeLen]); err == nil {
				if r, rest, err := decodeComponent(key[1+timeLen:]); err == nil {
					if ns, _, err := decodeComponent(rest); err == nil {
						return fmt.Sprintf("manifest-chunk[%s %s/%s]", t.Format(time.RFC3339Nano), r, ns)
					}
				}
			}
		}
	case tabLastRec:
		if r, _, err := decodeComponent(key[1:]); err == nil {
			return fmt.Sprintf("lastrec[%s]", r)
		}
	case tabMeta:
		return fmt.Sprintf("meta[%s]", key[1:])
	}
	return fmt.Sprintf("%x", key)
}
