package cli

import (
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // fixed zones independent of the host's zoneinfo
)

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func TestTimeParser(t *testing.T) {
	berlin := mustLoc(t, "Europe/Berlin")
	ny := mustLoc(t, "America/New_York")
	plus530 := time.FixedZone("IST", 5*3600+1800)
	// 2026-11-24 15:00:00 in Berlin (CET, +01:00).
	now := time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		in      string
		loc     *time.Location
		now     time.Time
		want    string // RFC 3339 UTC, or "" for an error
		wantErr string
	}{
		{name: "now", in: "now", want: "2026-11-24T14:00:00Z"},
		{name: "now upper case", in: "NOW", want: "2026-11-24T14:00:00Z"},
		{name: "minutes", in: "-20m", want: "2026-11-24T13:40:00Z"},
		{name: "hours and minutes", in: "-1h30m", want: "2026-11-24T12:30:00Z"},
		{name: "seconds", in: "-90s", want: "2026-11-24T13:58:30Z"},
		{name: "fractional hours", in: "-1.5h", want: "2026-11-24T12:30:00Z"},
		{name: "days", in: "-2d", want: "2026-11-22T14:00:00Z"},
		{name: "days and hours", in: "-1d12h", want: "2026-11-23T02:00:00Z"},
		{name: "now minus form", in: "now-20m", want: "2026-11-24T13:40:00Z"},
		{name: "surrounding spaces", in: "  -20m ", want: "2026-11-24T13:40:00Z"},
		{name: "zero duration is now", in: "-0s", want: "2026-11-24T14:00:00Z"},
		// 2d across the end of DST is 48 hours, not two calendar days.
		{name: "days are 24h across DST", in: "-2d", loc: berlin,
			now: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC), want: "2026-10-24T12:00:00Z"},

		{name: "rfc3339 Z", in: "2026-11-24T13:00:00Z", want: "2026-11-24T13:00:00Z"},
		{name: "rfc3339 offset", in: "2026-11-24T15:00:00+02:00", want: "2026-11-24T13:00:00Z"},
		{name: "rfc3339 nanos", in: "2026-11-24T13:00:00.123456789Z", want: "2026-11-24T13:00:00.123456789Z"},
		{name: "rfc3339 offset ignores location", in: "2026-11-24T08:00:00-05:00", loc: plus530, want: "2026-11-24T13:00:00Z"},

		{name: "local T seconds", in: "2026-11-24T14:00:00", loc: berlin, want: "2026-11-24T13:00:00Z"},
		{name: "local T minutes", in: "2026-11-24T14:00", loc: berlin, want: "2026-11-24T13:00:00Z"},
		{name: "local space minutes", in: "2026-11-24 14:00", loc: berlin, want: "2026-11-24T13:00:00Z"},
		{name: "local space seconds", in: "2026-11-24 13:59:30", loc: time.UTC, want: "2026-11-24T13:59:30Z"},
		{name: "local fraction", in: "2026-11-24T14:00:00.25", loc: berlin, want: "2026-11-24T13:00:00.25Z"},
		{name: "local half-hour zone", in: "2026-11-24 19:00", loc: plus530, want: "2026-11-24T13:30:00Z"},
		{name: "local summer time", in: "2026-07-01 12:00", loc: berlin, want: "2026-07-01T10:00:00Z"},

		{name: "time of day", in: "14:30", loc: berlin, want: "2026-11-24T13:30:00Z"},
		{name: "time of day seconds", in: "14:05:30", loc: berlin, want: "2026-11-24T13:05:30Z"},
		{name: "time of day fraction", in: "14:05:30.5", loc: berlin, want: "2026-11-24T13:05:30.5Z"},
		{name: "time of day single digit hour", in: "9:15", loc: berlin, want: "2026-11-24T08:15:00Z"},
		{name: "time of day equal to now", in: "15:00", loc: berlin, want: "2026-11-24T14:00:00Z"},
		{name: "time of day UTC", in: "13:59", loc: time.UTC, want: "2026-11-24T13:59:00Z"},
		// Today is a different date in New York (09:00 on the 24th) than
		// it would be in e.g. Tokyo; "today" follows the location.
		{name: "time of day uses local date", in: "08:00", loc: ny, want: "2026-11-24T13:00:00Z"},
		{name: "time of day just after local midnight", in: "00:30", loc: plus530,
			now: time.Date(2026, 11, 24, 19, 0, 0, 0, time.UTC), want: "2026-11-24T19:00:00Z"},

		{name: "future relative", in: "+20m", wantErr: "in the future"},
		{name: "future now plus", in: "now+5m", wantErr: "in the future"},
		{name: "future rfc3339", in: "2026-11-24T14:00:01Z", wantErr: "in the future"},
		{name: "future local", in: "2026-11-24 15:01", loc: berlin, wantErr: "in the future"},
		{name: "future time of day is not yesterday", in: "15:01", loc: berlin,
			wantErr: `"15:01" means today at 15:01:00 CET, which is in the future; for an earlier day give the date, e.g. "2026-11-23 15:01"`},
		{name: "missing minus", in: "20m", wantErr: "need a minus sign, e.g. -20m"},
		{name: "empty", in: "", wantErr: "empty time"},
		{name: "bare minus", in: "-", wantErr: "invalid relative time"},
		{name: "days after hours", in: "-1h2d", wantErr: "invalid relative time"},
		{name: "unknown unit", in: "-3w", wantErr: "invalid relative time"},
		{name: "garbage", in: "yesterday", wantErr: "cannot parse time"},
		{name: "bad hour", in: "25:00", wantErr: "invalid time of day"},
		{name: "bad minute", in: "12:60", wantErr: "invalid time of day"},
		{name: "date only", in: "2026-11-24", wantErr: "cannot parse time"},
		{name: "bad month", in: "2026-13-01 10:00", wantErr: "cannot parse time"},

		// Europe/Berlin springs forward 2026-03-29 02:00 -> 03:00 and falls
		// back 2026-10-25 03:00 -> 02:00.
		{name: "dst gap does not exist", in: "2026-03-29 02:30", loc: berlin, wantErr: "does not exist in zone Europe/Berlin"},
		{name: "dst overlap is ambiguous", in: "2026-10-25 02:30", loc: berlin,
			wantErr: "ambiguous in zone Europe/Berlin (it occurs twice because of a daylight saving change); give an RFC 3339 time with an offset, e.g. 2026-10-25T02:30:00+02:00 or 2026-10-25T02:30:00+01:00"},
		{name: "just before dst gap", in: "2026-03-29 01:59:59", loc: berlin, want: "2026-03-29T00:59:59Z"},
		{name: "just after dst gap", in: "2026-03-29 03:00", loc: berlin, want: "2026-03-29T01:00:00Z"},
		{name: "after dst overlap", in: "2026-10-25 03:00", loc: berlin, want: "2026-10-25T02:00:00Z"},
		{name: "overlap with explicit offset", in: "2026-10-25T02:30:00+01:00", loc: berlin, want: "2026-10-25T01:30:00Z"},
		{name: "time of day in dst gap", in: "02:30", loc: berlin,
			now: time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC), wantErr: "does not exist"},
		{name: "time of day in dst overlap", in: "02:30", loc: berlin,
			now: time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC), wantErr: "ambiguous"},
		{name: "new york overlap", in: "2026-11-01 01:30", loc: ny, wantErr: "ambiguous"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := now
			if !tc.now.IsZero() {
				n = tc.now
			}
			loc := tc.loc
			if loc == nil {
				loc = time.UTC
			}
			p := TimeParser{Now: func() time.Time { return n }, Location: loc}
			got, err := p.Parse(tc.in)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Parse(%q) = %s, want error containing %q", tc.in, wireTime(got), tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Parse(%q) error = %q, want it to contain %q", tc.in, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.in, err)
			}
			if w := wireTime(got); w != tc.want {
				t.Fatalf("Parse(%q) = %s, want %s", tc.in, w, tc.want)
			}
		})
	}
}

func TestWireTimeIsUTC(t *testing.T) {
	in := time.Date(2026, 11, 24, 15, 0, 0, 5, time.FixedZone("X", 3600))
	if got, want := wireTime(in), "2026-11-24T14:00:00.000000005Z"; got != want {
		t.Fatalf("wireTime = %s, want %s", got, want)
	}
}

func TestProtectRelativeTimes(t *testing.T) {
	in := []string{"-20m", "get", "pods", "-n", "ns", "-A", "--since", "-1h30m", "-l", "app=web", "--", "-5m"}
	got := protectRelativeTimes(in)
	want := []string{"now-20m", "get", "pods", "-n", "ns", "-A", "--since", "now-1h30m", "-l", "app=web", "--", "now-5m"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("protectRelativeTimes = %q, want %q", got, want)
	}
}
