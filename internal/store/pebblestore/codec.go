package pebblestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// Value framing: every value is
//
//	[1 byte codec version][4 bytes CRC32C, big endian][payload]
//
// The CRC covers the key, the codec byte and the payload, so a value that
// is damaged, truncated, or copied under the wrong key fails validation.
const (
	valueCodecV1 byte = 1
	valueHeader  int  = 5
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func valueCRC(key []byte, codec byte, payload []byte) uint32 {
	c := crc32.Update(0, castagnoli, key)
	c = crc32.Update(c, castagnoli, []byte{codec})
	return crc32.Update(c, castagnoli, payload)
}

// seal frames payload for storage under key.
func seal(key, payload []byte) []byte {
	out := make([]byte, valueHeader, valueHeader+len(payload))
	out[0] = valueCodecV1
	binary.BigEndian.PutUint32(out[1:], valueCRC(key, valueCodecV1, payload))
	return append(out, payload...)
}

// unseal validates a stored value and returns its payload. The payload
// aliases val.
func unseal(key, val []byte) ([]byte, error) {
	if len(val) < valueHeader {
		return nil, corruptf(key, "value has %d bytes, shorter than the %d-byte header", len(val), valueHeader)
	}
	if val[0] != valueCodecV1 {
		return nil, corruptf(key, "unknown value codec %d", val[0])
	}
	payload := val[valueHeader:]
	if got, want := valueCRC(key, val[0], payload), binary.BigEndian.Uint32(val[1:]); got != want {
		return nil, corruptf(key, "checksum mismatch (computed %08x, stored %08x)", got, want)
	}
	return payload, nil
}

func corruptf(key []byte, format string, args ...any) error {
	return fmt.Errorf("%w: %s: %s", store.ErrCorrupt, describeKey(key), fmt.Sprintf(format, args...))
}

var errShort = errors.New("payload truncated")

// reader decodes payload fields; the first error sticks.
type reader struct {
	b   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errShort
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) byte() byte {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 1 {
		r.err = errShort
		return 0
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c
}

func (r *reader) bytes(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if uint64(len(r.b)) < n {
		r.err = errShort
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) str() string { return string(r.bytes(r.uvarint())) }

func (r *reader) time() time.Time {
	b := r.bytes(timeLen)
	if r.err != nil {
		return time.Time{}
	}
	t, err := decodeTime(b)
	if err != nil {
		r.err = err
	}
	return t
}

func (r *reader) u64() uint64 {
	b := r.bytes(8)
	if r.err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// done reports the sticky error, or an error if bytes remain.
func (r *reader) done() error {
	if r.err == nil && len(r.b) != 0 {
		return fmt.Errorf("%d trailing bytes", len(r.b))
	}
	return r.err
}

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// Record (table V). Key and time come from the key.
//
//	seq uvarint | type u8 | origin u8 | uid str | resourceVersion str | object (rest)
func encodeRecord(r *model.Record) []byte {
	b := make([]byte, 0, 32+len(r.UID)+len(r.ResourceVersion)+len(r.Object))
	b = binary.AppendUvarint(b, r.Seq)
	b = append(b, byte(r.Type), byte(r.Origin))
	b = appendStr(b, r.UID)
	b = appendStr(b, r.ResourceVersion)
	return append(b, r.Object...)
}

// decodeVersion decodes a version key/value pair. rec.Object aliases val.
func decodeVersion(key, val []byte) (model.Record, error) {
	var rec model.Record
	k, t, err := parseVersionKey(key)
	if err != nil {
		return rec, corruptf(key, "bad version key: %v", err)
	}
	payload, err := unseal(key, val)
	if err != nil {
		return rec, err
	}
	r := reader{b: payload}
	rec.Seq = r.uvarint()
	rec.Type = model.ChangeType(r.byte())
	rec.Origin = model.Origin(r.byte())
	rec.UID = r.str()
	rec.ResourceVersion = r.str()
	if r.err != nil {
		return rec, corruptf(key, "decode record: %v", r.err)
	}
	rec.Object = r.b
	if len(rec.Object) == 0 {
		rec.Object = nil
	}
	rec.Key, rec.ObservedAt = k, t
	return rec, nil
}

// indexRef is the value of a time index entry (table T): enough to filter
// Changes and to drive compaction without reading the version.
type indexRef struct {
	key  model.ObjectKey
	typ  model.ChangeType
	seq  uint64
	time time.Time
}

// resource str | namespace str | name str | type u8 | seq uvarint
func encodeIndexRef(r *model.Record) []byte {
	b := make([]byte, 0, 16+len(r.Key.Resource)+len(r.Key.Namespace)+len(r.Key.Name))
	b = appendStr(b, r.Key.Resource)
	b = appendStr(b, r.Key.Namespace)
	b = appendStr(b, r.Key.Name)
	b = append(b, byte(r.Type))
	return binary.AppendUvarint(b, r.Seq)
}

func decodeIndexRef(key, val []byte) (indexRef, error) {
	var ref indexRef
	if len(key) != 1+timeLen || key[0] != tabIndex {
		return ref, corruptf(key, "bad index key")
	}
	t, err := decodeTime(key[1:])
	if err != nil {
		return ref, corruptf(key, "bad index key: %v", err)
	}
	payload, err := unseal(key, val)
	if err != nil {
		return ref, err
	}
	r := reader{b: payload}
	ref.key.Resource = r.str()
	ref.key.Namespace = r.str()
	ref.key.Name = r.str()
	ref.typ = model.ChangeType(r.byte())
	ref.seq = r.uvarint()
	if err := r.done(); err != nil {
		return ref, corruptf(key, "decode index entry: %v", err)
	}
	ref.time = t
	return ref, nil
}

// Coverage event (table C). Resource and Seq come from the key.
//
//	type u8 | at time | reason (rest)
func encodeCoverage(c *model.CoverageEvent) []byte {
	b := make([]byte, 0, 1+timeLen+len(c.Reason))
	b = append(b, byte(c.Type))
	b = appendTime(b, c.At)
	return append(b, c.Reason...)
}

func decodeCoverage(key, val []byte) (model.CoverageEvent, error) {
	var c model.CoverageEvent
	res, seq, err := parseCoverageKey(key)
	if err != nil {
		return c, corruptf(key, "bad coverage key: %v", err)
	}
	payload, err := unseal(key, val)
	if err != nil {
		return c, err
	}
	r := reader{b: payload}
	c.Type = model.CoverageType(r.byte())
	c.At = r.time()
	if r.err != nil {
		return c, corruptf(key, "decode coverage: %v", r.err)
	}
	c.Reason = string(r.b)
	c.Resource, c.Seq = res, seq
	return c, nil
}

// State (meta "state"): seq u64 | last assigned time.
func encodeState(seq uint64, last time.Time) []byte {
	return appendTime(binary.BigEndian.AppendUint64(nil, seq), last)
}

func decodeState(key, val []byte) (uint64, time.Time, error) {
	payload, err := unseal(key, val)
	if err != nil {
		return 0, time.Time{}, err
	}
	r := reader{b: payload}
	seq := r.u64()
	t := r.time()
	if err := r.done(); err != nil {
		return 0, time.Time{}, corruptf(key, "decode state: %v", err)
	}
	return seq, t, nil
}

func encodeTimeValue(t time.Time) []byte { return appendTime(nil, t) }

func decodeTimeValue(key, val []byte) (time.Time, error) {
	payload, err := unseal(key, val)
	if err != nil {
		return time.Time{}, err
	}
	t, err := decodeTime(payload)
	if err != nil {
		return time.Time{}, corruptf(key, "decode time: %v", err)
	}
	return t, nil
}

// manifestHeader (table N) describes one snapshot manifest at time S: for
// every resource that had records, the namespaces with entries and how
// many entries each chunk must contain.
//
// builtHorizon is the store horizon when the manifest was built; see
// view.listFromManifest for why it matters.
//
//	lastSeq uvarint | builtHorizon time | nres uvarint | { resource str | nns uvarint | { ns str | count uvarint } }
type manifestHeader struct {
	at           time.Time
	lastSeq      uint64
	builtHorizon time.Time
	resources    map[string][]nsCount // namespaces sorted
}

type nsCount struct {
	ns    string
	count int
}

func encodeManifestHeader(h *manifestHeader, order []string) []byte {
	var b []byte
	b = binary.AppendUvarint(b, h.lastSeq)
	b = appendTime(b, h.builtHorizon)
	b = binary.AppendUvarint(b, uint64(len(order)))
	for _, res := range order {
		b = appendStr(b, res)
		nss := h.resources[res]
		b = binary.AppendUvarint(b, uint64(len(nss)))
		for _, n := range nss {
			b = appendStr(b, n.ns)
			b = binary.AppendUvarint(b, uint64(n.count))
		}
	}
	return b
}

func decodeManifestHeader(key, val []byte) (*manifestHeader, error) {
	if len(key) != 1+timeLen || key[0] != tabManifest {
		return nil, corruptf(key, "bad manifest key")
	}
	at, err := decodeTime(key[1:])
	if err != nil {
		return nil, corruptf(key, "bad manifest key: %v", err)
	}
	payload, err := unseal(key, val)
	if err != nil {
		return nil, err
	}
	r := reader{b: payload}
	h := &manifestHeader{at: at, lastSeq: r.uvarint(), resources: map[string][]nsCount{}}
	h.builtHorizon = r.time()
	nres := r.uvarint()
	for i := uint64(0); i < nres && r.err == nil; i++ {
		res := r.str()
		nns := r.uvarint()
		var nss []nsCount
		for j := uint64(0); j < nns && r.err == nil; j++ {
			ns := r.str()
			nss = append(nss, nsCount{ns: ns, count: int(r.uvarint())})
		}
		h.resources[res] = nss
	}
	if err := r.done(); err != nil {
		return nil, corruptf(key, "decode manifest header: %v", err)
	}
	return h, nil
}

// manifestEntry is one key of a manifest chunk: the time of the key's
// latest record at S and whether that record is a delete.
type manifestEntry struct {
	name    string
	at      time.Time
	deleted bool
}

// Chunk (table S): count uvarint | { name str | time | flags u8 }, names
// sorted.
func encodeChunk(es []manifestEntry) []byte {
	b := binary.AppendUvarint(nil, uint64(len(es)))
	for _, e := range es {
		b = appendStr(b, e.name)
		b = appendTime(b, e.at)
		var f byte
		if e.deleted {
			f = 1
		}
		b = append(b, f)
	}
	return b
}

func decodeChunk(key, val []byte, want int) ([]manifestEntry, error) {
	payload, err := unseal(key, val)
	if err != nil {
		return nil, err
	}
	r := reader{b: payload}
	n := r.uvarint()
	if r.err == nil && n != uint64(want) {
		return nil, corruptf(key, "chunk has %d entries, header says %d", n, want)
	}
	es := make([]manifestEntry, 0, want)
	for i := uint64(0); i < n && r.err == nil; i++ {
		e := manifestEntry{name: r.str(), at: r.time()}
		e.deleted = r.byte() == 1
		if r.err == nil && len(es) > 0 && es[len(es)-1].name >= e.name {
			return nil, corruptf(key, "chunk entries out of order at %q", e.name)
		}
		es = append(es, e)
	}
	if err := r.done(); err != nil {
		return nil, corruptf(key, "decode chunk: %v", err)
	}
	return es, nil
}

// Format version (meta "format"): uvarint.
func decodeFormat(key, val []byte) (uint64, error) {
	payload, err := unseal(key, val)
	if err != nil {
		return 0, err
	}
	r := reader{b: payload}
	v := r.uvarint()
	if err := r.done(); err != nil {
		return 0, corruptf(key, "decode format version: %v", err)
	}
	return v, nil
}
