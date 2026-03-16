package backend

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

var ErrPEMParse = errors.New("failed to parse PEM block containing the private key")

type KalshiApiCredentials struct {
	privateKey *rsa.PrivateKey
	accessKey  string
	baseURL    *url.URL
}

func loadPrivateKeyFromFile(filePath string) (*rsa.PrivateKey, error) {
	// Read the private key file
	privKeyBytes, err := os.ReadFile(filePath)

	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(privKeyBytes)
	if block == nil {
		return nil, ErrPEMParse
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

func signPSS(privateKey *rsa.PrivateKey, message string) (string, error) {
	hash := sha256.New()
	hash.Write([]byte(message))
	hashedMessage := hash.Sum(nil)

	signature, err := rsa.SignPSS(rand.Reader, privateKey, crypto.SHA256, hashedMessage, nil)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(signature), nil
}

func (a *App) makeKalshiAuthenticatedRequest(method, path string, query map[string]string, payload interface{}) (*http.Response, error) {

	requestUrl := a.kalshi.baseURL.JoinPath(path)

	if query != nil {
		q := requestUrl.Query()
		for key, value := range query {
			q.Set(key, value)
			requestUrl.RawQuery = q.Encode()
		}
	}

	timestampString := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	msgString := timestampString + method + requestUrl.Path

	signature, err := signPSS(a.kalshi.privateKey, msgString)
	if err != nil {
		return nil, err
	}

	var body io.Reader
	if payload != nil {
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewBuffer(payloadBytes)
	}

	req, err := http.NewRequest(method, requestUrl.String(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("KALSHI-ACCESS-KEY", a.kalshi.accessKey)
	req.Header.Set("KALSHI-ACCESS-SIGNATURE", signature)
	req.Header.Set("KALSHI-ACCESS-TIMESTAMP", timestampString)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	return a.http.Do(req)
}
