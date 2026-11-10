package relay

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	c, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := "test_" + ID()
	if _, e = c.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		t.Fatal(e)
	}
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, e := OpenStore(ctx, u.String(), make([]byte, 32))
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		s.DB.Close()
		_, _ = c.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		c.Close(context.Background())
	})
	return s
}
func setupRoute(t *testing.T, s *Store, ep Endpoint) (Source, Route) {
	t.Helper()
	src := Source{ID: ID(), Name: "manual", Type: "manual", Enabled: true}
	_ = src.Validate()
	r := Route{ID: ID(), Name: "test", Enabled: true, SourceID: src.ID, EndpointIDs: []string{ep.ID}}
	for kind, v := range map[string]struct {
		id string
		v  any
	}{"sources": {src.ID, src}, "endpoints": {ep.ID, ep}, "routes": {r.ID, r}} {
		if e := s.Save(context.Background(), kind, v.id, v.v); e != nil {
			t.Fatal(e)
		}
	}
	return src, r
}
func TestInboxTransactionAndSnapshot(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ep := testEndpoint("https://example.com/original")
	src, route := setupRoute(t, s, ep)
	id, e := s.Admit(ctx, src.ID, "position-1", []byte(`{"kind":"paid"}`), map[string]any{})
	if e != nil {
		t.Fatal(e)
	}
	again, e := s.Admit(ctx, src.ID, "position-1", []byte(`{"kind":"paid"}`), nil)
	if e != nil || again != id {
		t.Fatalf("source redelivery not deduplicated: %v", e)
	}
	ep.URL = "https://example.com/changed"
	if e = s.Save(ctx, "endpoints", ep.ID, ep); e != nil {
		t.Fatal(e)
	}
	tasks, e := s.Claim(ctx, ep.ID, 10)
	if e != nil || len(tasks) != 1 {
		t.Fatalf("duplicate/lost tasks: %d %v", len(tasks), e)
	}
	if tasks[0].Endpoint.URL != "https://example.com/original" {
		t.Fatal("pending target snapshot mutated")
	}
	if e = s.Complete(ctx, tasks[0], Result{Status: "SUCCEEDED", HTTPStatus: 200}); e != nil {
		t.Fatal(e)
	}
	if e = s.Complete(ctx, tasks[0], Result{Status: "DEAD"}); e == nil {
		t.Fatal("stale completion overwrote final state")
	}
	replay, e := s.Replay(ctx, tasks[0].ID)
	if e != nil || replay == tasks[0].ID {
		t.Fatal("replay not a new task")
	}
	route.FilterPath = "$.missing"
	route.FilterValue = json.RawMessage(`1`)
	if e = s.Save(ctx, "routes", route.ID, route); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Admit(ctx, src.ID, "bad", []byte(`{}`), nil); e == nil {
		t.Fatal("invalid filter silently skipped")
	}
	var count int
	if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&count); e != nil || count != 1 {
		t.Fatalf("partial event committed: %d %v", count, e)
	}
}
func TestConcurrentClaimsAndRecovery(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ep := testEndpoint("https://example.com")
	src, _ := setupRoute(t, s, ep)
	for i := 0; i < 20; i++ {
		if _, e := s.Admit(ctx, src.ID, "", []byte("payload"), nil); e != nil {
			t.Fatal(e)
		}
	}
	var wg sync.WaitGroup
	ids := sync.Map{}
	var total atomic.Int32
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tasks, e := s.Claim(ctx, ep.ID, 10)
			if e != nil {
				t.Error(e)
				return
			}
			for _, d := range tasks {
				if _, ok := ids.LoadOrStore(d.ID, true); ok {
					t.Error("double claim")
				}
				total.Add(1)
			}
		}()
	}
	wg.Wait()
	if total.Load() != 20 {
		t.Fatalf("claimed %d", total.Load())
	}
	if _, e := s.DB.Exec(ctx, "UPDATE deliveries SET lease_until=now()-interval '1 second'"); e != nil {
		t.Fatal(e)
	}
	if e := s.Recover(ctx); e != nil {
		t.Fatal(e)
	}
	tasks, e := s.Claim(ctx, ep.ID, 30)
	if e != nil || len(tasks) != 20 {
		t.Fatalf("recovery lost tasks: %d %v", len(tasks), e)
	}
	for _, d := range tasks {
		if d.Attempts != 2 {
			t.Fatal(d.Attempts)
		}
	}
}
func TestEncryptedCredentialAndSignature(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	id, e := s.PutCredential(ctx, "hmac", map[string]string{"secret": "Jefe"})
	if e != nil {
		t.Fatal(e)
	}
	var cipher []byte
	_ = s.DB.QueryRow(ctx, "SELECT ciphertext FROM credentials WHERE id=$1", id).Scan(&cipher)
	if strings.Contains(string(cipher), "Jefe") {
		t.Fatal("credential stored plaintext")
	}
	ep := testEndpoint("https://example.com")
	ep.Auth = Auth{Type: "hmac_sha256", CredentialRef: id, Canonical: "what do ya want for nothing?"}
	sender := NewSender(s, Policy{}, nil)
	req, e := sender.Build(ctx, Delivery{ID: ID(), Endpoint: ep})
	if e != nil || req.Headers["X-Signature"] != "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843" {
		t.Fatalf("signature: %v %v", req, e)
	}
}
func TestHTTPPrefixAndAuthentication(t *testing.T) {
	s := testStore(t)
	app := &Server{Store: s, Sources: NewSources(s), Sender: NewSender(s, localPolicy(), nil), Prefix: "bridge", Token: strings.Repeat("x", 32)}
	server := httptest.NewServer(app.Handler())
	defer server.Close()
	cases := []struct {
		path   string
		status int
		kind   string
	}{{"/bridge/", 200, "text/html"}, {"/bridge/sources", 200, "text/html"}, {"/bridge/sources/deep", 200, "text/html"}, {"/bridge/assets/missing.js", 404, "text/plain"}, {"/api/bridge/v1/sources", 401, "application/json"}, {"/api/bridge/v1/health/ready", 200, "application/json"}, {"/relay/", 404, "text/plain"}}
	for _, tc := range cases {
		resp, e := http.Get(server.URL + tc.path)
		if e != nil {
			t.Fatal(e)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.status || !strings.HasPrefix(resp.Header.Get("Content-Type"), tc.kind) {
			t.Errorf("%s: %d %s", tc.path, resp.StatusCode, resp.Header.Get("Content-Type"))
		}
	}
	req, _ := http.NewRequest("GET", server.URL+"/api/bridge/v1/missing", nil)
	req.Header.Set("Authorization", "Bearer "+app.Token)
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Fatal("API fallback returned frontend")
	}
}
func TestDispatcherEndToEnd(t *testing.T) {
	s := testStore(t)
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer target.Close()
	ep := testEndpoint(target.URL)
	src, _ := setupRoute(t, s, ep)
	if _, e := s.Admit(context.Background(), src.ID, "", []byte("hello"), nil); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&Dispatcher{Store: s, Sender: NewSender(s, localPolicy(), nil), MaxInflight: 8}).Run(ctx)
	}()
	defer func() { cancel(); <-done }()
	deadline := time.After(40 * time.Second)
	for {
		select {
		case <-deadline:
			list, _ := s.Deliveries(context.Background(), "")
			t.Fatalf("dispatcher did not recover and retry; calls=%d tasks=%+v", calls.Load(), list)
		case <-time.After(100 * time.Millisecond):
			m, e := s.Stats(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if m["SUCCEEDED"] == 1 {
				if calls.Load() != 2 {
					t.Fatalf("unexpected attempts: %d", calls.Load())
				}
				return
			}
		}
	}
}

func TestRetentionPreservesDedupAndPending(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	ep := testEndpoint("https://example.com")
	src, _ := setupRoute(t, s, ep)
	id, e := s.Admit(ctx, src.ID, "retained-key", []byte("test"), nil)
	if e != nil {
		t.Fatal(e)
	}
	tasks, e := s.Claim(ctx, ep.ID, 1)
	if e != nil || len(tasks) != 1 {
		t.Fatal(e)
	}
	if e = s.Complete(ctx, tasks[0], Result{Status: "SUCCEEDED"}); e != nil {
		t.Fatal(e)
	}
	_, _ = s.DB.Exec(ctx, "UPDATE deliveries SET created_at=now()-interval '8 days'")
	_, _ = s.DB.Exec(ctx, "UPDATE events SET created_at=now()-interval '8 days'")
	if e = s.Cleanup(ctx); e != nil {
		t.Fatal(e)
	}
	var n int
	if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&n); e != nil || n != 0 {
		t.Fatalf("terminal event not cleaned: %d %v", n, e)
	}
	again, e := s.Admit(ctx, src.ID, "retained-key", []byte("test"), nil)
	if e != nil || again != id {
		t.Fatalf("dedup lost after retention: %v", e)
	}
	if _, e = s.Admit(ctx, src.ID, "", []byte("pending"), nil); e != nil {
		t.Fatal(e)
	}
	_, _ = s.DB.Exec(ctx, "UPDATE events SET created_at=now()-interval '8 days'")
	if e = s.Cleanup(ctx); e != nil {
		t.Fatal(e)
	}
	if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&n); e != nil || n != 1 {
		t.Fatalf("pending raw event removed: %d %v", n, e)
	}
}

func TestPreviewNeverLoadsOAuthChain(t *testing.T) {
	s := testStore(t)
	app := &Server{Store: s, Sources: NewSources(s), Sender: NewSender(s, localPolicy(), nil), Prefix: "relay", Token: strings.Repeat("x", 32)}
	server := httptest.NewServer(app.Handler())
	defer server.Close()
	ep := testEndpoint("https://example.com")
	ep.AuthChain = []Auth{{Type: "oauth2", CredentialRef: "does-not-exist"}}
	body, _ := json.Marshal(map[string]any{"endpoint": ep, "body": "hello"})
	req, _ := http.NewRequest("POST", server.URL+"/api/relay/v1/debug/preview", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+app.Token)
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal("preview attempted to access credential or token service")
	}
}

func TestExternalConnector(t *testing.T) {
	s := testStore(t)
	var mode atomic.Int32
	var received atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		if r.Header.Get("X-Partner-Signature") != "sdk-result" {
			t.Error("connector request header missing")
		}
		w.WriteHeader(204)
	}))
	defer target.Close()
	connector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode.Load() == 2 {
			w.WriteHeader(503)
			return
		}
		var job map[string]any
		if json.NewDecoder(r.Body).Decode(&job) != nil {
			t.Error("invalid connector job")
		}
		url := target.URL
		if mode.Load() == 1 {
			url = "https://unauthorized.example.com"
		}
		_ = json.NewEncoder(w).Encode(BuiltRequest{Method: "POST", URL: url, Headers: map[string]string{"X-Partner-Signature": "sdk-result"}, Body: "SDK generated body"})
	}))
	defer connector.Close()
	ep := testEndpoint(target.URL)
	ep.BodyMode = "connector"
	ep.ConnectorURL = connector.URL
	if e := ep.Validate(); e != nil {
		t.Fatal(e)
	}
	sender := NewSender(s, localPolicy(), nil)
	d := Delivery{ID: ID(), Endpoint: ep, Event: Event{Raw: []byte("input")}}
	if result := sender.Send(context.Background(), d); result.Status != "SUCCEEDED" {
		t.Fatalf("connector delivery failed: %+v", result)
	}
	mode.Store(1)
	if _, e := sender.Build(context.Background(), d); e == nil {
		t.Fatal("connector changed authorized target origin")
	}
	mode.Store(2)
	if result := sender.Send(context.Background(), d); result.Status != "RETRY_WAIT" {
		t.Fatal("temporary connector failure not retried")
	}
	if received.Load() != 1 {
		t.Fatal("unexpected target requests")
	}
}
