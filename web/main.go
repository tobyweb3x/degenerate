package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"web/protos"
	backend "web/server"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {

	var environmentalVariables backend.EnvConfig
	if err := backend.LoadEnv(&environmentalVariables); err != nil {
		log.Fatalf("error loading environmental variables: %s\n", err.Error()) // exit
	}

	//  database
	var databaseUrl string
	if environmentalVariables.Build == "production" {
		databaseUrl = environmentalVariables.Database.ProdPrivate
	} else {
		databaseUrl = environmentalVariables.Database.Dev
	}

	conn, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		log.Fatalf("unable to create db connection pool: %s\n", err.Error())
	}
	defer conn.Close()

	// app
	app, err := backend.NewApp(conn, &environmentalVariables)
	if err != nil {
		log.Fatalf("unable to create app: %s\n", err.Error())
	}

	// grpc
	grpcListener, err := net.Listen("tcp", ":"+environmentalVariables.GrpcPort)
	if err != nil {
		log.Printf("grpc: failed to listen on port %s: %s", environmentalVariables.GrpcPort, err.Error())
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    15 * time.Second,
			Timeout: 5 * time.Second,
		}),
	)
	myServer := backend.NewGrpcServer(app)
	protos.RegisterEsuOdaraServer(grpcServer, myServer)

	go func() {
		log.Printf("🚀 gRPC server listening on 0.0.0.0:%s on mode: %s\n", environmentalVariables.GrpcPort, environmentalVariables.Build)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Printf("failed to serve gRPC: %s", err.Error())
		}
	}()

	// http
	httpServer := http.Server{
		Addr:    fmt.Sprintf(":%s", environmentalVariables.HttpPort),
		Handler: app.Routes(),
	}
	go func() {
		log.Printf("http server is running on http://localhost:%s on mode: %s\n", environmentalVariables.HttpPort, environmentalVariables.Build)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("failed to server http: %s", err.Error())
		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	<-done
	grpcServer.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful server shutdown Failed: %s", err.Error())
	}

	log.Println("SERVER STOPPED GRACEFULLY")
}
