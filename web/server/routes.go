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

	r.Post("/resolve/hit/submit/{correlationId}", a.resolveHitSubmit)
	r.Post("/resolve/arb/submit/confirm/{correlationId}", a.resolveArbConfirm)
	r.Post("/resolve/arb/submit/confirm&run/{correlationId}", a.resolveArbConfirmAndRun)
	
	r.Get("/resolve/hit/{correlationId}", a.resolveHitPage)
	r.Get("/resolve/arb/{correlationId}", a.resolveArbPage)

	r.Get("/hits", a.similarityHitsPage)
	r.Get("/arbs", a.arbsPage)

	r.Post("/delete/hit/{correlationId}", a.deleteHit)
	r.Post("/delete/arb/{correlationId}", a.deleteArb)

	r.Get("/market/json/{platform}/{ticker}", a.getMarket)
	r.Get("/book/json/{platform}/{tokenId}", a.getBook)

	var staticFiles = fs.FS(frontend.AssetsDir)
	staticFs, _ := fs.Sub(staticFiles, "public/assets")
	r.Handle("/assets/*", http.StripPrefix("/assets/", http.FileServer(http.FS(staticFs))))
	return r
}
