package cli

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Recorded objects are written by whoever can create them in the cluster
// (an Event message, a container status reason), so every string printed
// to a terminal is sanitized: control characters could otherwise move the
// cursor, rewrite earlier lines, change the window title, or forge table
// rows. kubectl's printers.EscapeTerminal only handles ESC and CR; this
// escapes every C0 and C1 control character and DEL, and invalid UTF-8.

// safeCell makes s safe as one table cell: no control characters at all,
// so tabs and newlines cannot split it into extra columns or rows.
func safeCell(s string) string { return escapeControls(s, false) }

// safeText makes s safe for a line of terminal text; newlines are kept
// because Chronos's own multi-line messages use them.
func safeText(s string) string { return escapeControls(s, true) }

func needsEscape(r rune, keepNewline bool) bool {
	switch {
	case r == '\n':
		return !keepNewline
	case r < 0x20, r == 0x7f, r >= 0x80 && r <= 0x9f, r == utf8.RuneError:
		return true
	}
	return false
}

func escapeControls(s string, keepNewline bool) string {
	clean := true
	for _, r := range s {
		if needsEscape(r, keepNewline) {
			clean = false
			break
		}
	}
	if clean {
		return s
	}
	var b strings.Builder
	for i, w := 0, 0; i < len(s); i += w {
		r, size := utf8.DecodeRuneInString(s[i:])
		w = size
		switch {
		case r == utf8.RuneError && size == 1:
			fmt.Fprintf(&b, `\x%02x`, s[i])
		case !needsEscape(r, keepNewline):
			b.WriteRune(r)
		case r == '\x1b':
			b.WriteString("^[")
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r < 0x80:
			fmt.Fprintf(&b, `\x%02x`, r)
		default:
			fmt.Fprintf(&b, `\u%04x`, r)
		}
	}
	return b.String()
}

func safeCells(cells []string) []string {
	for i, c := range cells {
		cells[i] = safeCell(c)
	}
	return cells
}
