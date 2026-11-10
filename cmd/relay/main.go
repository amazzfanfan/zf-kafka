package main

import (
	"context"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"relay/internal/relay"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "executor" {
		if relay.RunExecutor(os.Stdin, os.Stdout) != nil {
			os.Exit(1)
		}
		return
	}
	if e := run(); e != nil {
		slog.Error("relay stopped", "error", e.Error())
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	prefix := env("APP_PREFIX", "relay")
	if e := relay.ValidatePrefix(prefix); e != nil {
		return e
	}
	token := os.Getenv("RELAY_ADMIN_TOKEN")
	if len(token) < 24 {
		return &configError{"RELAY_ADMIN_TOKEN must be at least 24 characters"}
	}
	key, e := hex.DecodeString(os.Getenv("RELAY_MASTER_KEY"))
	if e != nil || len(key) != 32 {
		return &configError{"RELAY_MASTER_KEY must be 64 hex characters (persist it for credential recovery)"}
	}
	s, e := relay.OpenStore(ctx, os.Getenv("DATABASE_URL"), key)
	if e != nil {
		return &configError{"database unavailable; configure DATABASE_URL"}
	}
	defer s.DB.Close()
	if v := os.Getenv("RELAY_STORAGE_BUDGET_BYTES"); v != "" {
		n, e := strconv.ParseInt(v, 10, 64)
		if e != nil || n < 64*1024*1024 {
			return &configError{"RELAY_STORAGE_BUDGET_BYTES must be at least 64 MiB"}
		}
		s.MaxStorageBytes = n
	}
	if e = s.Migrate(ctx); e != nil {
		return e
	}
	var policy relay.Policy
	for _, v := range strings.Split(os.Getenv("RELAY_ALLOWED_CIDRS"), ",") {
		if strings.TrimSpace(v) == "" {
			continue
		}
		p, e := netip.ParsePrefix(strings.TrimSpace(v))
		if e != nil {
			return e
		}
		policy.Allowed = append(policy.Allowed, p)
	}
	trusted := os.Getenv("RELAY_ENABLE_TRUSTED_SCRIPTS") == "true"
	var scripts *relay.ScriptPool
	if trusted {
		scripts = relay.NewScriptPool(2)
		defer scripts.Close()
	}
	sender := relay.NewSender(s, policy, scripts)
	sources := relay.NewSources(s)
	role := env("RELAY_ROLE", "all")
	if role != "all" && role != "api" && role != "ingest" && role != "dispatcher" {
		return &configError{"RELAY_ROLE must be all, api, ingest or dispatcher"}
	}
	n, _ := strconv.Atoi(env("RELAY_MAX_INFLIGHT", "256"))
	if n < 1 || n > 4096 {
		return &configError{"RELAY_MAX_INFLIGHT must be 1..4096"}
	}
	var wg sync.WaitGroup
	if role == "all" || role == "ingest" {
		wg.Add(1)
		go func() { defer wg.Done(); sources.Run(ctx) }()
	}
	if role == "all" || role == "dispatcher" {
		d := &relay.Dispatcher{Store: s, Sender: sender, MaxInflight: n}
		wg.Add(1)
		go func() { defer wg.Done(); d.Run(ctx) }()
	}
	var server *http.Server
	var serverErr error
	if role == "all" || role == "api" {
		app := &relay.Server{Store: s, Sources: sources, Sender: sender, Prefix: prefix, Token: token, TrustedScripts: trusted}
		server = &http.Server{Addr: env("RELAY_LISTEN", "127.0.0.1:8080"), Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768}
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("relay listening", "address", server.Addr, "page", "/"+prefix+"/", "api", "/api/"+prefix+"/v1")
			if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
				serverErr = e
				slog.Error("HTTP server failed")
				stop()
			}
		}()
	}
	<-ctx.Done()
	if server != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		_ = server.Shutdown(shutdown)
		cancel()
	}
	wg.Wait()
	return serverErr
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type configError struct{ message string }

func (e *configError) Error() string { return e.message }
