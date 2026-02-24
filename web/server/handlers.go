package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"web/protos"
	frontend "web/view/pages"

	"github.com/go-chi/chi"
)

func (a *App) dashboardPage(w http.ResponseWriter, r *http.Request) {

	if err := frontend.DashboardPage().Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) needsResolvePage(w http.ResponseWriter, r *http.Request) {

	arbs, err := a.db.GetAllCrossArbs(context.Background())
	if err != nil {
		return
	}

	templModel, err := frontend.FromCrossPlatformArbs(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := frontend.ResolvePage(templModel).Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) resolvePage(w http.ResponseWriter, r *http.Request) {
	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url part paramter"))
		return
	}

	needsResolve, err := a.db.GetNeedsResolveByCorrelationID(context.Background(), correlationId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			a.clientError(w, http.StatusNotFound, errors.New("entry does not exist"))
			return
		}
	
		a.serverError(w, err)
		return
	}

	var hit protos.SimilarityHit
	if err := json.Unmarshal(needsResolve.SimilarityHit, &hit); err != nil {
		a.serverError(w, err)
		return
	}
}
