package backend

import (
	"fmt"
	"os"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type EnvConfig struct {
	Database struct {
		ProdPublic  string `env:"DATABASE_URL_PROD_PUBLIC"`
		ProdPrivate string `env:"DATABASE_URL_PROD_PRIVATE"`
		Dev         string `env:"DATABASE_URL_DEV"`
	}

	KalshiApiCredentials struct {
		ApiKeyID           string `env:"KALSHI_API_KEY_ID"`
		PrivateKeyFilePath string `env:"KALSHI_PK_FILE_PATH"`
	}

	Build    string `env:"BUILD"`
	HttpPort string `env:"HTTP_PORT"`
	GrpcPort string `env:"GRPC_PORT"`
}

func LoadEnv(cfg *EnvConfig) error {

	if os.Getenv("DOCKER_ENV") == "" {
		if err := godotenv.Load(".env"); err != nil {
			return fmt.Errorf("error loading .env file: %s", err.Error())
		}

	}

	return env.Parse(cfg)
}
