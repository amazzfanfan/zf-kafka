package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "executor" {
		if RunExecutor(os.Stdin, os.Stdout) != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
func localPolicy() Policy {
	return Policy{Allowed: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}}
}
func testEndpoint(address string) Endpoint {
	ep := Endpoint{ID: ID(), Name: "test", Enabled: true, URL: address, Method: "POST", BodyMode: "raw", AllowPrivate: true}
	if e := ep.Validate(); e != nil {
		panic(e)
	}
	return ep
}
func TestPrefixAndSourceIsolation(t *testing.T) {
	for _, p := range []string{"", "api", "../relay", "relay/x"} {
		if ValidatePrefix(p) == nil {
			t.Fatalf("unsafe prefix %q", p)
		}
	}
	s := Source{Name: "test", Type: "kafka", Brokers: []string{"localhost:9092"}, Topics: []string{"business"}, Group: "business", Start: "latest"}
	if s.Validate() == nil {
		t.Fatal("business consumer group accepted")
	}
	s = Source{Name: "test", Type: "rabbitmq", URL: "amqp://localhost/", Queue: "business"}
	if s.Validate() == nil {
		t.Fatal("business queue accepted")
	}
	s.Queue = "relay.copy"
	s.Declare = true
	if s.Validate() == nil {
		t.Fatal("default exchange cannot mirror")
	}
}
func TestNetworkPolicy(t *testing.T) {
	p := localPolicy()
	for _, addr := range []string{"127.0.0.1", "::1", "169.254.169.254", "10.0.0.1", "::ffff:127.0.0.1"} {
		if p.allowed(netip.MustParseAddr(addr), false) {
			t.Fatalf("private address allowed: %s", addr)
		}
	}
	if p.allowed(netip.MustParseAddr("169.254.169.254"), true) {
		t.Fatal("private toggle bypasses CIDR allowlist")
	}
	if !p.allowed(netip.MustParseAddr("127.0.0.1"), true) {
		t.Fatal("explicit local allowlist rejected")
	}
}
func TestRawBytesAndResponseContract(t *testing.T) {
	raw := []byte("\x00\xff raw\n{ \"big\":9007199254740993 }")
	calls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body := make([]byte, len(raw))
		n, _ := r.Body.Read(body)
		if string(body[:n]) != string(raw) {
			t.Error("raw message mutated")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"code":7}`))
	}))
	defer target.Close()
	ep := testEndpoint(target.URL)
	ep.SuccessPath = "$.code"
	ep.SuccessValue = json.RawMessage(`0`)
	sender := NewSender(nil, localPolicy(), nil)
	d := Delivery{ID: ID(), Endpoint: ep, Event: Event{Raw: raw}}
	if r := sender.Send(context.Background(), d); r.Status != "DEAD" || r.HTTPStatus != 200 {
		t.Fatalf("HTTP 200 business error accepted: %+v", r)
	}
	ep.SuccessValue = json.RawMessage(`7`)
	d.Endpoint = ep
	if r := sender.Send(context.Background(), d); r.Status != "SUCCEEDED" {
		t.Fatalf("valid contract rejected: %+v", r)
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}
func TestRedirectAndRetryAfter(t *testing.T) {
	hits := 0
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(204) }))
	defer blocked.Close()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, blocked.URL, 302) }))
	defer target.Close()
	d := Delivery{ID: ID(), Endpoint: testEndpoint(target.URL)}
	s := NewSender(nil, localPolicy(), nil)
	if r := s.Send(context.Background(), d); r.Status != "DEAD" || hits != 0 {
		t.Fatal("redirect followed")
	}
	retry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Header().Set("Retry-After", "2"); w.WriteHeader(429) }))
	defer retry.Close()
	d.Endpoint = testEndpoint(retry.URL)
	r := s.Send(context.Background(), d)
	if r.Status != "RETRY_WAIT" || r.RetryAfter != 2*time.Second {
		t.Fatalf("bad retry: %+v", r)
	}
}
func TestTemplatePrecisionAndMissingField(t *testing.T) {
	s := NewSender(nil, localPolicy(), nil)
	ep := testEndpoint("https://example.com/hook")
	ep.BodyMode = "template"
	ep.Body = `{"id":{{json .JSON.id}},"price":{{json .JSON.price}}}`
	d := Delivery{ID: ID(), Endpoint: ep, Event: Event{Raw: []byte(`{"id":9007199254740993,"price":"12.30"}`)}}
	r, e := s.Build(context.Background(), d)
	if e != nil || !strings.Contains(r.Body, "9007199254740993") {
		t.Fatalf("precision lost: %s %v", r.Body, e)
	}
	d.Endpoint.Body = `{{.JSON.missing}}`
	if _, e = s.Build(context.Background(), d); e == nil {
		t.Fatal("missing template value silently accepted")
	}
}
func TestScriptTimeoutAndIsolation(t *testing.T) {
	p := NewScriptPool(1)
	defer p.Close()
	ctx := context.Background()
	v, e := p.Run(ctx, ScriptJob{Script: `var seen=0;function buildRequest(ctx){seen++;return {n:seen,value:ctx.value};}`, Function: "buildRequest", Context: map[string]any{"value": "9007199254740993"}})
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(v)
	if !strings.Contains(string(b), `"n":1`) {
		t.Fatal(v)
	}
	v, e = p.Run(ctx, ScriptJob{Script: `function buildRequest(ctx){return typeof seen + ':' + typeof require + ':' + typeof Date}`, Function: "buildRequest"})
	if e != nil || v != "undefined:undefined:undefined" {
		t.Fatalf("state/host leaked: %v %v", v, e)
	}
	start := time.Now()
	_, e = p.Run(ctx, ScriptJob{Script: `function buildRequest(){while(true){}}`, Function: "buildRequest"})
	if e == nil || time.Since(start) > time.Second {
		t.Fatal("infinite loop not interrupted")
	}
	v, e = p.Run(ctx, ScriptJob{Script: `function buildRequest(){return crypto.hmacSHA256('what do ya want for nothing?', 'hex')}`, Function: "buildRequest", Secret: "Jefe"})
	if e != nil || v != "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843" {
		t.Fatalf("HMAC golden sample failed: %v %v", v, e)
	}
}
