// Example external adapter. Replace the request construction with a partner SDK
// when its protocol cannot be expressed by templates or the restricted JS API.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	secret, token := os.Getenv("CONNECTOR_SIGNING_SECRET"), os.Getenv("CONNECTOR_TOKEN")
	if len(secret) < 16 || len(token) < 24 {
		log.Fatal("set CONNECTOR_SIGNING_SECRET (>=16 chars) and CONNECTOR_TOKEN (>=24 chars)")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /build", func(w http.ResponseWriter, r *http.Request) {
		a, b := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))), sha256.Sum256([]byte(token))
		if subtle.ConstantTimeCompare(a[:], b[:]) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 2*1048576)
		var job struct {
			Version   int    `json:"version"`
			Timestamp string `json:"timestamp"`
			Nonce     string `json:"nonce"`
			Event     struct {
				Raw []byte `json:"raw"`
			} `json:"event"`
			Target struct {
				URL    string `json:"url"`
				Method string `json:"method"`
			} `json:"target"`
		}
		if json.NewDecoder(r.Body).Decode(&job) != nil || job.Version != 1 {
			http.Error(w, "invalid job", 400)
			return
		}
		signature := hmac.New(sha256.New, []byte(secret))
		signature.Write([]byte(job.Timestamp + "\n"))
		signature.Write(job.Event.Raw)
		body, _ := json.Marshal(map[string]any{"payload": string(job.Event.Raw), "signature": hex.EncodeToString(signature.Sum(nil)), "timestamp": job.Timestamp, "nonce": job.Nonce})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"url": job.Target.URL, "method": job.Target.Method, "headers": map[string]string{"Content-Type": "application/json"}, "body": string(body)})
	})
	s := http.Server{Addr: "127.0.0.1:8090", Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
	log.Fatal(s.ListenAndServe())
}
