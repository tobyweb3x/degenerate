package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"web/protos"
	frontend "web/view/pages"

	"github.com/go-chi/chi/v5"
)

const (
	HxRequest = "HX-Request"
)

func isHXRequest(r *http.Request) bool {
	headerValue := r.Header.Get(HxRequest)
	if headerValue == "" {
		return false
	}

	if hxRequest, err := strconv.ParseBool(headerValue); (err == nil) && hxRequest {
		return true
	}

	return false
}

func (a *App) dashboardPage(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.DashboardPage().Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) needsResolvePage(w http.ResponseWriter, r *http.Request) {

	arbs, err := a.db.GetAllCrossArbs(context.Background())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.FromCrossPlatformArbs(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.NeedsResolvePartial(templModels).Render(context.Background(), w); err != nil {
			a.serverError(w, err)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.NeedsResolvePage(templModels).Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) resolvePage(w http.ResponseWriter, r *http.Request) {

	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
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

	templModel, err := frontend.FromCrossPlatformArbs(needsResolve)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(templModel) == 0 {
		a.serverError(w, errors.New("SimilarityHit data for this correlationId was empty"))
		return
	}

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.ResolvePartial(templModel[0]).Render(context.TODO(), w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.ResolvePage(templModel[0]).Render(context.Background(), w); err != nil {
		a.serverError(w, err)
		return
	}
}

// Struct to hold the parsed data
type ResolveRequest struct {
	CorrelationID string
	Selections    []LegMapping
}

type LegMapping struct {
	MatchUUID string
	AnchorLeg int
	MatchLeg  int
}

func (a *App) resolveSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form", http.StatusBadRequest)
		return
	}

	correlationID := r.FormValue("correlation_id")
	rawSelections := r.Form["selections[]"]

	if len(rawSelections) == 0 {
		http.Error(w, "No selections made", http.StatusBadRequest)
		return
	}

	var mappings []LegMapping

	for _, raw := range rawSelections {
		fmt.Println("raw", raw)
		// Clean Format: "uuid|0|1"
		parts := strings.Split(raw, "|")
		if len(parts) != 3 {
			fmt.Printf("invalid selection format: %s\n", raw)
			continue
		}

		anchorIdx, err1 := strconv.Atoi(parts[1])
		matchIdx, err2 := strconv.Atoi(parts[2])

		if err1 != nil || err2 != nil {
			fmt.Printf("invalid indices: %s\n", raw)
			continue
		}

		mappings = append(mappings, LegMapping{
			MatchUUID: parts[0],
			AnchorLeg: anchorIdx,
			MatchLeg:  matchIdx,
		})
	}

	// Logic ...
	fmt.Printf("Processing Resolve for %s (%d items)\n", correlationID, len(mappings))
	for _, m := range mappings {
		fmt.Printf("  - Resolve %s: [%d] -> [%d]\n", m.MatchUUID[:8], m.AnchorLeg, m.MatchLeg)
	}

	w.Header().Set("Content-Type", "text/html")
	http.Redirect(w, r, "/resolve", http.StatusSeeOther)
}

func (a *App) deleteNeedsResolve(w http.ResponseWriter, r *http.Request) {
	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	if err := a.db.DeleteNeedsResolve(context.Background(), correlationId); err != nil {
		a.serverError(w, fmt.Errorf("error delection %s: %w", correlationId, err))
		return
	}

	arbs, err := a.db.GetAllCrossArbs(context.Background())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.FromCrossPlatformArbs(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.NeedsResolvePartial(templModels).Render(context.Background(), w); err != nil {
		a.serverError(w, err)
		return
	}
}
