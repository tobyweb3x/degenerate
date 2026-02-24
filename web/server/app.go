package backend

import (
	"web/protos"
	"web/service/repository"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
)

type App struct {
	GrpcComms chan *protos.Ebo
	db        *repository.Repository
}

func NewApp(conn *pgxpool.Pool) *App {
	return &App{
		GrpcComms: make(chan *protos.Ebo, 100),
		db:        repository.NewService(conn),
	}
}

func (a *App) closeGrpcComms() {
	close(a.GrpcComms)
}

var protoMarshaler = protojson.MarshalOptions{
	UseProtoNames:   true, // Keep snake_case keys (optional)
	EmitUnpopulated: true, // Save default values (optional)
}
