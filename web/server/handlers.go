package backend

import (
	"context"
	"net/http"
	frontend "web/view/pages"
)

func (a *App) dashboardPage(w http.ResponseWriter, r *http.Request) {

	if err := frontend.DashboardPage().Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *App) resolvePage(w http.ResponseWriter, r *http.Request) {

	if err := frontend.ResolvePage().Render(context.TODO(), w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
