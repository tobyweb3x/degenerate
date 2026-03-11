package backend

import (
	"web/protos"
	"web/service/repository"

	"github.com/jackc/pgx/v5/pgxpool"
)

type App struct {
	GrpcComms chan *protos.Ebo
	db        *repository.Repository
}

func NewApp(conn *pgxpool.Pool) *App {
	return &App{
		GrpcComms: make(chan *protos.Ebo, 512),
		db:        repository.NewService(conn),
	}
}

func (a *App) closeGrpcComms() {
	close(a.GrpcComms)
}
