package server

import (
	"bytes"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

// contentTypes pins the types that matter for the dashboard so they do not
// depend on the host's mime.types (module scripts are refused by browsers
// under a wrong type once nosniff is set).
var contentTypes = map[string]string{
	".html":  "text/html; charset=utf-8",
	".js":    "text/javascript; charset=utf-8",
	".mjs":   "text/javascript; charset=utf-8",
	".css":   "text/css; charset=utf-8",
	".json":  "application/json",
	".map":   "application/json",
	".svg":   "image/svg+xml",
	".png":   "image/png",
	".ico":   "image/x-icon",
	".woff2": "font/woff2",
	".wasm":  "application/wasm",
	".txt":   "text/plain; charset=utf-8",
}

func contentType(name string) string {
	ext := strings.ToLower(path.Ext(name))
	if t, ok := contentTypes[ext]; ok {
		return t
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return "application/octet-stream"
}

// spa serves a single-page app build. Existing files are served as is.
// A missing path is answered with index.html when the client is navigating
// (Accept includes text/html), so client-side routes work on reload; other
// misses (a script or image that does not exist) get a 404, which is easier
// to debug than HTML served as JavaScript. Resource names may contain dots,
// so the decision cannot be made from the file extension.
//
// Caching: build outputs under assets/ carry content hashes (Vite's
// default), so they are immutable; everything else, index.html in
// particular, must be revalidated so a new release is picked up.
func spa(fsys fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		if fs.ValidPath(name) && serveFile(w, r, fsys, name) {
			return
		}
		if !strings.Contains(r.Header.Get("Accept"), "text/html") {
			writeError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		if !serveFile(w, r, fsys, "index.html") {
			writeError(w, http.StatusNotFound, "not_found", "not found")
		}
	})
}

// serveFile serves name if it is a regular file and reports whether it did.
func serveFile(w http.ResponseWriter, r *http.Request, fsys fs.FS, name string) bool {
	f, err := fsys.Open(name)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		return false
	}
	var content io.ReadSeeker
	if rs, ok := f.(io.ReadSeeker); ok {
		content = rs
	} else {
		b, err := io.ReadAll(f)
		if err != nil {
			return false
		}
		content = bytes.NewReader(b)
	}
	h := w.Header()
	h.Set("Content-Type", contentType(name))
	if strings.HasPrefix(name, "assets/") {
		h.Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		h.Set("Cache-Control", "no-cache")
	}
	// embed.FS reports a zero ModTime; ServeContent then omits
	// Last-Modified, which is correct for content we cannot date.
	http.ServeContent(w, r, name, st.ModTime(), content)
	return true
}
