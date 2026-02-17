package backend

import (
	"io/fs"
	"net/http"

	frontend "web/view"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
)

func (a *App) Routes() *chi.Mux {
	r := chi.NewRouter()

	// r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Compress(5))
	r.MethodNotAllowed(r.MethodNotAllowedHandler())
	r.NotFound(r.NotFoundHandler())

	r.Get("/", a.dashboardPage)
	r.Get("/dashboard", a.dashboardPage)
	r.Get("/resolve", a.resolvePage)

	var staticFiles = fs.FS(frontend.AssetsDir)
	staticFs, _ := fs.Sub(staticFiles, "public/assets")
	r.Handle("/assets/*", http.StripPrefix("/assets/", http.FileServer(http.FS(staticFs))))
	return r
}
