package backend

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type EnvConfig struct {
	Database struct {
		ProdPrivate string `env:"DATABASE_URL_PROD_PRIVATE"`
		ProdPublic  string `env:"DATABASE_URL_PROD_PUBLIC"`
		Dev         string `env:"DATABASE_URL_DEV"`
	}

	Build    string `env:"BUILD"`
	HttpPort string `env:"HTTP_PORT"`
	GrpcPort string `env:"GRPC_PORT"`
}

func LoadEnv(cfg *EnvConfig) error {

	if err := godotenv.Load(".env"); err != nil {
		fmt.Printf("error loading .env file: %v", err)
	}

	return env.Parse(cfg)
}
