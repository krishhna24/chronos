package cli

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// TimeParser resolves the time arguments of kubectl at (ADR 0002): relative
// forms against the client's clock, zone-less forms in Location. Every
// accepted time is at or before Now; the server never sees a relative or
// zone-less time.
type TimeParser struct {
	// Now is the client's clock.
	Now func() time.Time
	// Location interprets times written without an offset (the local zone,
	// or UTC with --utc).
	Location *time.Location
}

// timeHelp lists the accepted forms; it is appended to parse errors.
const timeHelp = "use a relative time (-20m, -1h30m, -2d), now, RFC 3339 " +
	"(2026-11-24T14:00:00Z or with an offset), a local date and time " +
	"(2026-11-24T14:00:00, \"2026-11-24 14:00\"), or a time of day today (14:00, 14:05:30)"

var (
	// relativeRe: optional whole days followed by an optional Go duration.
	relativeRe = regexp.MustCompile(`^(?:(\d+)d)?((?:\d+(?:\.\d+)?(?:h|m|s|ms|us|µs|ns))*)$`)
	// timeOfDayRe: HH:MM or HH:MM:SS (fraction allowed).
	timeOfDayRe = regexp.MustCompile(`^(\d{1,2}):(\d{2})(?::(\d{2})(\.\d{1,9})?)?$`)
)

// localLayouts are date-time forms without an offset. Fractional seconds
// after the seconds field are accepted by time.Parse even though the
// layout does not mention them.
var localLayouts = []string{
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
}

// Parse resolves s to an absolute instant. The result is never after Now.
func (p TimeParser) Parse(s string) (time.Time, error) {
	now := p.Now()
	loc := p.Location
	if loc == nil {
		loc = time.Local
	}
	in := strings.TrimSpace(s)
	if in == "" {
		return time.Time{}, fmt.Errorf("empty time; %s", timeHelp)
	}

	t, err := p.parse(in, now, loc)
	if err != nil {
		return time.Time{}, err
	}
	if t.After(now) {
		return time.Time{}, fmt.Errorf("time %q (%s) is in the future; Chronos can only answer about the past",
			s, t.In(loc).Format(time.RFC3339))
	}
	return t, nil
}

func (p TimeParser) parse(in string, now time.Time, loc *time.Location) (time.Time, error) {
	lower := strings.ToLower(in)
	switch {
	case lower == "now":
		return now, nil
	case strings.HasPrefix(lower, "now-"):
		return relative(in, lower[len("now-"):], now)
	case strings.HasPrefix(lower, "now+"), strings.HasPrefix(lower, "+"):
		return time.Time{}, fmt.Errorf("time %q is in the future; Chronos can only answer about the past", in)
	case strings.HasPrefix(lower, "-"):
		return relative(in, lower[1:], now)
	}

	if t, err := time.Parse(time.RFC3339Nano, in); err == nil {
		return t, nil
	}
	for _, layout := range localLayouts {
		// Parse in UTC to get the wall-clock reading exactly as written;
		// ParseInLocation would already have normalized a DST gap.
		if w, err := time.Parse(layout, in); err == nil {
			return wallClock(in, w, loc)
		}
	}
	if m := timeOfDayRe.FindStringSubmatch(in); m != nil {
		return timeOfDay(in, m, now, loc)
	}
	if relativeRe.MatchString(lower) && lower != "" {
		return time.Time{}, fmt.Errorf("time %q: relative times count back from now and need a minus sign, e.g. -%s", in, in)
	}
	return time.Time{}, fmt.Errorf("cannot parse time %q; %s", in, timeHelp)
}

// relative parses "20m", "1h30m", "2d", "1d12h" (the part after "-"). A day
// is 24 hours, not a calendar day: -1d across a DST change is still 24h.
func relative(orig, body string, now time.Time) (time.Time, error) {
	m := relativeRe.FindStringSubmatch(body)
	if body == "" || m == nil {
		return time.Time{}, fmt.Errorf("invalid relative time %q; %s", orig, timeHelp)
	}
	var d time.Duration
	if m[1] != "" {
		days, err := strconv.Atoi(m[1])
		if err != nil || days > 365*100 {
			return time.Time{}, fmt.Errorf("invalid relative time %q: too many days", orig)
		}
		d = time.Duration(days) * 24 * time.Hour
	}
	if m[2] != "" {
		rest, err := time.ParseDuration(m[2])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid relative time %q: %w", orig, err)
		}
		// time.Duration tops out near 292 years; adding past it wraps
		// negative and would read as a time in the future.
		if rest < 0 || d > math.MaxInt64-rest {
			return time.Time{}, fmt.Errorf("invalid relative time %q: too far in the past", orig)
		}
		d += rest
	}
	return now.Add(-d), nil
}

// timeOfDay resolves HH:MM[:SS] to that wall-clock time today in loc. A
// time later than now is an error rather than silently meaning yesterday.
func timeOfDay(orig string, m []string, now time.Time, loc *time.Location) (time.Time, error) {
	h, _ := strconv.Atoi(m[1])
	mi, _ := strconv.Atoi(m[2])
	sec := 0
	if m[3] != "" {
		sec, _ = strconv.Atoi(m[3])
	}
	nsec := 0
	if m[4] != "" {
		frac := (m[4][1:] + "000000000")[:9]
		nsec, _ = strconv.Atoi(frac)
	}
	if h > 23 || mi > 59 || sec > 59 {
		return time.Time{}, fmt.Errorf("invalid time of day %q", orig)
	}
	y, mo, d := now.In(loc).Date()
	t, err := wallClock(orig, time.Date(y, mo, d, h, mi, sec, nsec, time.UTC), loc)
	if err != nil {
		return time.Time{}, err
	}
	if t.After(now) {
		return time.Time{}, fmt.Errorf("time %q means today at %s, which is in the future; "+
			"for an earlier day give the date, e.g. \"%s %s\"",
			orig, t.Format("15:04:05 MST"), now.In(loc).AddDate(0, 0, -1).Format("2006-01-02"), orig)
	}
	return t, nil
}

// wallClock resolves a wall-clock reading (the fields of wall, which is in
// UTC only as a carrier) to the instant it names in loc. A reading inside a
// DST spring-forward gap names no instant and one inside a fall-back overlap
// names two; Go's time.Date silently picks one in both cases, so they are
// rejected with an explanation instead.
func wallClock(orig string, wall time.Time, loc *time.Location) (time.Time, error) {
	y, mo, d := wall.Date()
	h, mi, s := wall.Clock()

	// The offsets in effect a day either side of the reading cover both
	// sides of any transition near it (zones never change twice in 48h).
	var found []time.Time
	for _, probe := range []time.Time{wall.Add(-24 * time.Hour), wall, wall.Add(24 * time.Hour)} {
		_, off := probe.In(loc).Zone()
		cand := wall.Add(-time.Duration(off) * time.Second).In(loc)
		cy, cmo, cd := cand.Date()
		ch, cmi, cs := cand.Clock()
		if cy != y || cmo != mo || cd != d || ch != h || cmi != mi || cs != s {
			continue
		}
		dup := false
		for _, f := range found {
			dup = dup || f.Equal(cand)
		}
		if !dup {
			found = append(found, cand)
		}
	}
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return time.Time{}, fmt.Errorf("time %q does not exist in zone %s (skipped by a daylight saving change); "+
			"give an RFC 3339 time with an offset", orig, loc)
	default:
		if found[1].Before(found[0]) {
			found[0], found[1] = found[1], found[0]
		}
		return time.Time{}, fmt.Errorf("time %q is ambiguous in zone %s (it occurs twice because of a daylight saving change); "+
			"give an RFC 3339 time with an offset, e.g. %s or %s",
			orig, loc, found[0].Format(time.RFC3339), found[1].Format(time.RFC3339))
	}
}

// wireTime is the form every time takes on the wire: RFC 3339 in UTC.
func wireTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
