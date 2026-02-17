package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	backend "web/server"
)

func main() {

	app, port := backend.NewApp(), "8090"
	server := http.Server{
		Addr:    fmt.Sprintf(":%v", port),
		Handler: app.Routes(),
		// ErrorLog:    slog.NewLogLogger(jsonLogger, slog.LevelError),
		// IdleTimeout: time.Minute,
		// ReadTimeout:  5 * time.Second,
		// WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("server is running on http://localhost:%v\n", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Panic("Server error: " + err.Error())
		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	<-done
	close(done)

	if err := server.Shutdown(context.TODO()); err != nil {
		log.Panic("Graceful server shutdown Failed: " + err.Error())
	}

	log.Println("SERVER STOPPED GRACEFULLY")
}
