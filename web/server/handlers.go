package backend

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

func (a *App) similarityHitsPage(w http.ResponseWriter, r *http.Request) {

	arbs, err := a.db.GetRecentCrossHits(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.ToCrossPlatformHits(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	counts := countPlatformAchorsfromHit(templModels)

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.SimilarityHitPartial(templModels, counts).Render(r.Context(), w); err != nil {
			a.serverError(w, err)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.SimilarityHitsPage(templModels, counts).Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) arbsPage(w http.ResponseWriter, r *http.Request) {

	arbs, err := a.db.GetRecentCrossArbs(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.ToCrossPlatformArbs(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	platformCounts := countPlatformAchorsFromArb(templModels)
	statusCount := countArbStatus(arbs...)

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.ArbsPartial(templModels, platformCounts, statusCount).Render(r.Context(), w); err != nil {
			a.serverError(w, err)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.ArbsPage(templModels, platformCounts, statusCount).Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) resolveHitPage(w http.ResponseWriter, r *http.Request) {

	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	needsResolve, err := a.db.GetSimilarityHitByCorrelationID(r.Context(), correlationId)
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
		if err := frontend.ResolveHitPartial(templModel[0]).Render(context.TODO(), w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.ResolveHitPage(templModel[0]).Render(r.Context(), w); err != nil {
		a.serverError(w, err)
		return
	}
}

func (a *App) resolveArbPage(w http.ResponseWriter, r *http.Request) {

	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	needsResolve, err := a.db.GetArbByCorrelationID(r.Context(), correlationId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			a.clientError(w, http.StatusNotFound, errors.New("entry does not exist"))
			return
		}

		a.serverError(w, err)
		return
	}

	var arb protos.Arb
	if err := frontend.ProtoUnMarshaler.Unmarshal(needsResolve.Arbs, &arb); err != nil {
		a.serverError(w, err)
		return
	}

	templModel, err := frontend.ToCrossPlatformArbs(needsResolve)
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
		if err := frontend.ResolveArbPartial(templModel[0]).Render(context.TODO(), w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.ResolveArbPage(templModel[0]).Render(r.Context(), w); err != nil {
		a.serverError(w, err)
		return
	}
}

func (a *App) resolveHitSubmit(w http.ResponseWriter, r *http.Request) {

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

func (a *App) resolveArbConfirm(w http.ResponseWriter, r *http.Request) {

	correlationID := chi.URLParam(r, "correlationId")
	if correlationID == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	if err := a.db.UpdateArbConfirmedToTrue(r.Context(), correlationID); err != nil {
		a.serverError(w, fmt.Errorf("could not update arb status: %s", err.Error()))
		return
	}

	w.Header().Set("Content-Type", "text/html")
	http.Redirect(w, r, "/arbs", http.StatusSeeOther)
}

func (a *App) resolveArbConfirmAndRun(w http.ResponseWriter, r *http.Request) {

	correlationID := chi.URLParam(r, "correlationId")
	if correlationID == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	arb, err := a.db.GetArbByCorrelationID(r.Context(), correlationID)
	if err != nil {
		a.serverError(w, fmt.Errorf("could not update arb status: %s", err.Error()))
		return
	}

	if len(arb.Arbs) == 0 {
		a.serverError(w, errors.New("arb.Arbs was empty"))
		return
	}

	var arb_ebo protos.Arb
	if err := frontend.ProtoUnMarshaler.Unmarshal(arb.Arbs, &arb_ebo); err != nil {
		a.serverError(w, err)
		return
	}

	ebo := &protos.ServerEbo{
		CorrelationId: arb.CorrelationID,
		Action: &protos.ServerEbo_ConfirmedAndRun{
			ConfirmedAndRun: &arb_ebo,
		},
		FoundAt: time.Now().UnixMilli(),
	}

	select {
	case a.GrpcComms <- ebo:
		if _, err = a.db.UpdateArbStatus(r.Context(), correlationID); err != nil {
			a.serverError(w, fmt.Errorf("could not update arb status: %s", err.Error()))
			return
		}

	case <-time.After(50 * time.Millisecond):
		a.serverError(w, errors.New("GrpcComms send timeout"))
		return
	}

	w.Header().Set("Content-Type", "text/html")
	http.Redirect(w, r, "/arbs", http.StatusSeeOther)
}

func (a *App) deleteHit(w http.ResponseWriter, r *http.Request) {
	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	if err := a.db.DeleteSimilarityHit(r.Context(), correlationId); err != nil {
		a.serverError(w, fmt.Errorf("error delection %s: %w", correlationId, err))
		return
	}

	hits, err := a.db.GetRecentCrossHits(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.ToCrossPlatformHits(hits...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	counts := countPlatformAchorsfromHit(templModels)

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.SimilarityHitPartial(templModels, counts).Render(r.Context(), w); err != nil {
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

	if err := a.db.DeleteArb(r.Context(), correlationId); err != nil {
		a.serverError(w, fmt.Errorf("error delection %s: %w", correlationId, err))
		return
	}

	arbs, err := a.db.GetRecentCrossArbs(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}

	templModels, err := frontend.ToCrossPlatformArbs(arbs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	platformCounts := countPlatformAchorsFromArb(templModels)
	statuscCount := countArbStatus(arbs...)

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.ArbsPartial(templModels, platformCounts, statuscCount).Render(r.Context(), w); err != nil {
		a.serverError(w, err)
		return
	}
}

const polymarketGetSlugURL = "https://gamma-api.polymarket.com/markets/slug"
const KalshiGetTickerURL = "https://api.elections.kalshi.com/trade-api/v2/markets"

func (a *App) getMarket(w http.ResponseWriter, r *http.Request) {
	platformParam := chi.URLParam(r, "platform")
	tickerParam := strings.TrimSpace(chi.URLParam(r, "ticker"))

	platform, ok := protos.Platform_value[platformParam]
	if !ok {
		a.clientError(w, http.StatusNotImplemented, fmt.Errorf("platform (%s) not supported", platformParam))
		return
	}

	var url string

	switch platform {
	case int32(protos.Platform_POLYMARKET):
		url = fmt.Sprintf("%s/%s", polymarketGetSlugURL, tickerParam)

	case int32(protos.Platform_KALSHI):
		url = fmt.Sprintf("%s/%s", KalshiGetTickerURL, tickerParam)

	default:
		a.clientError(w, http.StatusBadRequest, fmt.Errorf("platform (%s) not supported", platformParam))
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	if err != nil {
		a.serverError(w, err)
		return
	}

	resp, err := a.http.Do(req)
	if err != nil {
		a.serverError(w, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		a.serverError(w, fmt.Errorf("upstream returned %d", resp.StatusCode))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.serverError(w, err)
		return
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err != nil {
		a.serverError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err = frontend.JsonDetails(pretty.String()).Render(r.Context(), w); err != nil {
		a.serverError(w, err)
		return
	}
}

const polymarketGetBook = "https://clob.polymarket.com/book?token_id="

func (a *App) getBook(w http.ResponseWriter, r *http.Request) {
	platformParam := chi.URLParam(r, "platform")
	tokenId := strings.TrimSpace(chi.URLParam(r, "tokenId"))

	platform, ok := protos.Platform_value[platformParam]
	if !ok {
		a.clientError(w, http.StatusNotImplemented, fmt.Errorf("platform (%s) not supported", platformParam))
		return
	}

	var (
		resp *http.Response
		err  error
	)
	switch platform {
	case int32(protos.Platform_POLYMARKET):
		req, err := http.NewRequestWithContext(
			r.Context(),
			http.MethodGet,
			fmt.Sprintf("%s%s", polymarketGetBook, tokenId),
			nil,
		)
		if err != nil {
			a.serverError(w, err)
			return
		}

		if resp, err = a.http.Do(req); err != nil {
			a.serverError(w, err)
			return
		}

		defer resp.Body.Close()

	case int32(protos.Platform_KALSHI):
		if resp, err = a.makeKalshiAuthenticatedRequest(
			http.MethodGet,
			fmt.Sprintf("/trade-api/v2/markets/%s/orderbook", tokenId),
			map[string]string{"depth": "100"},
			nil,
		); err != nil {
			a.serverError(w, err)
			return
		}

		defer resp.Body.Close()

	default:
		a.clientError(w, http.StatusBadRequest, fmt.Errorf("platform (%s) not supported", platformParam))
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		a.serverError(w, err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		a.serverError(w, fmt.Errorf("upstream returned %d:%s", resp.StatusCode, string(body)))
		return
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err != nil {
		a.serverError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err = frontend.JsonDetails(pretty.String()).Render(r.Context(), w); err != nil {
		a.serverError(w, err)
		return
	}
}
