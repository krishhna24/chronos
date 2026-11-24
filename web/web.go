// Package web embeds the Chronos dashboard.
//
// The dashboard is built by `make web` (Vite writes web/dist). Only
// web/dist/.keep is tracked in git, so `go build ./...` works on a fresh
// checkout; in that case FS serves a placeholder page explaining how to
// build the dashboard instead of an empty directory.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

//go:embed placeholder
var placeholder embed.FS

// FS returns the dashboard file tree rooted at its index.html: the built
// dashboard when web/dist was populated at compile time, otherwise a
// single placeholder page.
func FS() fs.FS {
	if Built() {
		return mustSub(dist, "dist")
	}
	return mustSub(placeholder, "placeholder")
}

// Built reports whether the real dashboard was embedded at compile time.
func Built() bool {
	_, err := fs.Stat(dist, "dist/index.html")
	return err == nil
}

func mustSub(f fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		// Unreachable: dir is a literal embedded directory.
		panic(err)
	}
	return sub
}
