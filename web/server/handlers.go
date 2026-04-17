package backend

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

	fresh_hits := make([]frontend.TemplateModel[protos.ClientEbo_CrossPlatformHit], 0, len(templModels))
	past_hits := make([]string, 0, len(templModels))
	now := time.Now()

	for _, v := range templModels {
		anchor_close_time := time.UnixMilli(v.Payload.CrossPlatformHit.Anchor.CloseTimeMs)
		if anchor_close_time.Before(now) {
			past_hits = append(past_hits, v.CorrelationId)
			continue
		}

		fresh_hits = append(fresh_hits, v)
	}

	go func(correction_ids []string) {
		for _, correctionID := range correction_ids {
			if err := a.db.SoftDeleteSimilarityHit(context.Background(), correctionID); err != nil {
				log.Println("error deleting past_hits")
			}
		}
	}(past_hits)

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.SimilarityHitsPartial(templModels).Render(r.Context(), w); err != nil {
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

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.ArbsPartial(templModels).Render(r.Context(), w); err != nil {
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
		if err := a.db.SoftDeleteSimilarityHit(r.Context(), correlationID); err != nil {
			a.serverError(w, err)
			return
		}
	case <-time.After(50 * time.Millisecond):
		a.serverError(w, errors.New("GrpcComms send timeout"))
		return
	}

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
		ActionAt: time.Now().UnixMilli(),
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

	http.Redirect(w, r, "/arbs", http.StatusSeeOther)
}

func (a *App) softDeleteHit(w http.ResponseWriter, r *http.Request) {
	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	if err := a.db.SoftDeleteSimilarityHit(r.Context(), correlationId); err != nil {
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
		a.serverError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.SimilarityHitsPartial(templModels).Render(r.Context(), w); err != nil {
		a.serverError(w, err)
		return
	}
}

func (a *App) bulkSoftDeleteHits(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.clientError(w, http.StatusBadRequest, fmt.Errorf("error parsing form request: %w", err))
		return
	}

	correlationIDs := r.Form["ids"]
	if len(correlationIDs) == 0 {
		a.clientError(w, http.StatusBadRequest, errors.New("got empty correction_ids"))
		return
	}

	if err := a.db.SoftDeleteSimilarityHitsBulk(r.Context(), correlationIDs); err != nil {
		a.serverError(w, fmt.Errorf("error bulk deleting hitd: %w", err))
		return
	}

	http.Redirect(w, r, "/hits", http.StatusSeeOther)
}
func (a *App) deleteArb(w http.ResponseWriter, r *http.Request) {
	correlationId := chi.URLParam(r, "correlationId")
	if correlationId == "" {
		a.clientError(w, http.StatusBadRequest, errors.New("missing url path paramter"))
		return
	}

	ebo := &protos.ServerEbo{
		Action: &protos.ServerEbo_DeleteRunningArbs{
			DeleteRunningArbs: &protos.DeleteRunningArbRequest{
				CorrelationIds: []string{correlationId},
			},
		},
	}

	select {
	case a.GrpcComms <- ebo:
		if err := a.db.DeleteArb(r.Context(), correlationId); err != nil {
			a.serverError(w, fmt.Errorf("error delection %s: %w", correlationId, err))
			return
		}
	case <-time.After(50 * time.Millisecond):
		a.serverError(w, errors.New("GrpcComms send timeout"))
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

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.ArbsPartial(templModels).Render(r.Context(), w); err != nil {
		a.serverError(w, err)
		return
	}
}

func (a *App) bulkDeleteArbs(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.clientError(w, http.StatusBadRequest, fmt.Errorf("error parsing form request: %w", err))
		return
	}

	correlationIDs := r.Form["ids"]
	if len(correlationIDs) == 0 {
		a.clientError(w, http.StatusBadRequest, errors.New("got empty correction_ids"))
		return
	}

	ebo := &protos.ServerEbo{
		Action: &protos.ServerEbo_DeleteRunningArbs{
			DeleteRunningArbs: &protos.DeleteRunningArbRequest{
				CorrelationIds: correlationIDs,
			},
		},
	}

	select {
	case a.GrpcComms <- ebo:
		if err := a.db.DeleteArbsBulk(r.Context(), correlationIDs); err != nil {
			a.serverError(w, fmt.Errorf("error bulk deleting hitd: %w", err))
			return
		}
	case <-time.After(50 * time.Millisecond):
		a.serverError(w, errors.New("GrpcComms send timeout"))
		return
	}

	http.Redirect(w, r, "/arbs", http.StatusSeeOther)
}

func (a *App) ordersPage(w http.ResponseWriter, r *http.Request) {

	orders, err := a.db.GetOrderWithExcess(r.Context(), 5_000)
	if err != nil {
		a.serverError(w, err)
		return
	}

	if isHXRequest(r) {
		w.Header().Set("Content-Type", "text/html")
		if err := frontend.OrdersPartial(orders).Render(r.Context(), w); err != nil {
			a.serverError(w, err)
			return
		}
		return
	}

	w.Header().Set("Content-Type", "text/html")
	if err := frontend.OrdersPage(orders).Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
