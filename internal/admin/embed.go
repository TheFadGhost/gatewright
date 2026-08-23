package admin

import (
	"embed"
	"net/http"
	"strings"
)

//go:embed web/index.html web/app.js web/styles.css
var webFS embed.FS

var assetTypes = map[string]string{
	"app.js":     "text/javascript; charset=utf-8",
	"styles.css": "text/css; charset=utf-8",
}

func serveEmbedded(w http.ResponseWriter, r *http.Request, name, contentType string) {
	data, err := webFS.ReadFile("web/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data)
}

// handleIndex serves the embedded dashboard shell at /admin/.
func handleIndex(w http.ResponseWriter, r *http.Request) {
	serveEmbedded(w, r, "index.html", "text/html; charset=utf-8")
}

// handleAsset serves app.js and styles.css under /admin/assets/.
func handleAsset(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/admin/assets/")
	ct, ok := assetTypes[name]
	if !ok || strings.ContainsRune(name, '/') {
		http.NotFound(w, r)
		return
	}
	serveEmbedded(w, r, name, ct)
}
