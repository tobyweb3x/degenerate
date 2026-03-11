package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
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

func (a *App) similarityHitPage(w http.ResponseWriter, r *http.Request) {

	arbs, err := a.db.GetRecentCrossHits(context.Background())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.ToCrossPlatformHits(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.SimilarityHitPartial(templModels).Render(context.Background(), w); err != nil {
			a.serverError(w, err)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.SimilarityHitsPage(templModels).Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) arbsPage(w http.ResponseWriter, r *http.Request) {

	arbs, err := a.db.GetRecentCrossArbs(context.Background())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.ToCrossPlatformArbs(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.ArbsPartial(templModels).Render(context.Background(), w); err != nil {
			a.serverError(w, err)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.ArbsPage(templModels).Render(context.TODO(), w); err != nil {
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

	needsResolve, err := a.db.GetSimilarityHitByCorrelationID(context.Background(), correlationId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			a.clientError(w, http.StatusNotFound, errors.New("entry does not exist"))
			return
		}

		a.serverError(w, err)
		return
	}

	var hit protos.SimilarityHit
	if err := frontend.ProtoUnMarshaler.Unmarshal(needsResolve.SimilarityHit, &hit); err != nil {
		a.serverError(w, err)
		return
	}

	templModel, err := frontend.ToCrossPlatformHits(needsResolve)
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

func (a *App) resolveSubmit(w http.ResponseWriter, r *http.Request) {

	correlationID := chi.URLParam(r, "correlationId")
	if correlationID == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "failed to parse form", http.StatusBadRequest)
		return
	}

	rawSelections := r.Form["selections[]"]
	if len(rawSelections) == 0 {
		a.clientError(w, http.StatusBadRequest, errors.New("No selections made"))
		return
	}

	var mappings []LegMapping
	{
		for _, raw := range rawSelections {
			fmt.Printf("raw---%s\n\n", raw)
			parts := strings.Split(raw, "|") // expected Format: "uuid|0|1"
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
	}

	if len(mappings) == 0 {
		a.clientError(w, http.StatusBadRequest, errors.New("LegMapping was empty"))
		return
	}

	hit, err := a.db.GetSimilarityHitByCorrelationID(r.Context(), correlationID)
	if err != nil {
		a.serverError(w, err)
		return
	}

	if len(hit.SimilarityHit) == 0 {
		a.serverError(w, errors.New("similarity_hit was empty"))
		return
	}

	var similarityHit protos.SimilarityHit
	if err := frontend.ProtoUnMarshaler.Unmarshal(hit.SimilarityHit, &similarityHit); err != nil {
		a.serverError(w, err)
		return
	}

	discoveredArb, err := buildDiscoveredArbs(&similarityHit, mappings)
	if err != nil {
		a.serverError(w, err)
		return
	}

	ebo := newCrossPlatformArbDiscovery(discoveredArb)

	select {
	case a.GrpcComms <- ebo:
		if err := a.db.DeleteSimilarityHit(r.Context(), correlationID); err != nil {
			a.serverError(w, err)
			return
		}
	case <-time.After(50 * time.Millisecond):
		a.serverError(w, errors.New("GrpcComms send timeout"))
		return
	}

	w.Header().Set("Content-Type", "text/html")
	http.Redirect(w, r, "/hits", http.StatusSeeOther)
}

func (a *App) deleteHit(w http.ResponseWriter, r *http.Request) {
	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	if err := a.db.DeleteSimilarityHit(context.Background(), correlationId); err != nil {
		a.serverError(w, fmt.Errorf("error delection %s: %w", correlationId, err))
		return
	}

	hits, err := a.db.GetRecentCrossHits(context.Background())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.ToCrossPlatformHits(hits...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.SimilarityHitPartial(templModels).Render(context.Background(), w); err != nil {
		a.serverError(w, err)
		return
	}
}

func (a *App) deleteArb(w http.ResponseWriter, r *http.Request) {
	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	if err := a.db.DeleteArb(context.Background(), correlationId); err != nil {
		a.serverError(w, fmt.Errorf("error delection %s: %w", correlationId, err))
		return
	}

	arbs, err := a.db.GetRecentCrossArbs(context.Background())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.ToCrossPlatformArbs(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.ArbsPartial(templModels).Render(context.Background(), w); err != nil {
		a.serverError(w, err)
		return
	}
}
