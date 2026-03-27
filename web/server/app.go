package backend

import (
	"context"
	"crypto/rsa"
	"log"
	"net/http"
	"net/url"
	"time"
	"web/protos"
	"web/service/repository"

	"github.com/jackc/pgx/v5/pgxpool"
)

type App struct {
	GrpcComms chan *protos.ServerEbo
	db        *repository.Repository
	http      *http.Client
	kalshi    *KalshiApiCredentials
}

func NewApp(conn *pgxpool.Pool, configs *EnvConfig) (*App, error) {
	var (
		privateKey *rsa.PrivateKey
		err        error
	)

	if privateKey, err = loadPrivateKeyFromFile(configs.KalshiApiCredentials.PrivateKeyFilePath); err != nil {
		return nil, err
	}

	baseURL, err := url.Parse("https://api.elections.kalshi.com")
	if err != nil {
		return nil, err
	}

	return &App{
		GrpcComms: make(chan *protos.ServerEbo, 512),
		db:        repository.NewService(conn),
		http:      http.DefaultClient,
		kalshi: &KalshiApiCredentials{
			privateKey: privateKey,
			accessKey:  configs.KalshiApiCredentials.ApiKeyID,
			baseURL:    baseURL,
		},
	}, nil
}

func (a *App) closeGrpcComms() {
	close(a.GrpcComms)
}

func (a *App) CleanOldHitFromDbCron(ctx context.Context, deleteFrom time.Time) {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	run := func(deleteFrom time.Time) {
		log.Println("🧹 Running daily database cleanup...")
		err := a.db.HardDeleteHit(ctx, deleteFrom)
		if err != nil {
			log.Printf("Error cleaning database: %v", err)
			return
		}

		log.Println("✅ Database cleanup successful")
	}

	run(deleteFrom)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping DB cleanup cron")
			return

		case <-ticker.C:
			run(deleteFrom)
			
		}
	}
}
