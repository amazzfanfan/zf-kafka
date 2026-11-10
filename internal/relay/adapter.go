package relay

import (
	"bytes"
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"
)

type Policy struct{ Allowed []netip.Prefix }

var nonPublic = []netip.Prefix{netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001:db8::/32")}

func (p Policy) allowed(ip netip.Addr, private bool) bool {
	ip = ip.Unmap()
	if ip == netip.MustParseAddr("169.254.169.254") || ip == netip.MustParseAddr("100.100.100.200") || ip == netip.MustParseAddr("fd00:ec2::254") {
		return false
	}
	restricted := !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast()
	for _, p := range nonPublic {
		if p.Contains(ip) {
			restricted = true
		}
	}
	if restricted {
		if !private {
			return false
		}
		for _, r := range p.Allowed {
			if r.Contains(ip) {
				return true
			}
		}
		return false
	}
	return true
}
func (p Policy) Dial(private bool) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		ips, e := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if e != nil {
			return nil, errors.New("DNS resolution failed")
		}
		if len(ips) == 0 {
			return nil, errors.New("DNS returned no address")
		}
		for _, ip := range ips {
			if !p.allowed(ip, private) {
				return nil, errors.New("destination blocked by network policy")
			}
		}
		d := net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}
		for _, ip := range ips {
			c, err := d.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return c, nil
			}
		}
		return nil, errors.New("connection failed")
	}
}

type BuiltRequest struct {
	Method     string            `json:"method"`
	URL        string            `json:"url"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
	BodyBase64 string            `json:"body_base64,omitempty"`
}
type temporaryError struct{ message string }

func (e *temporaryError) Error() string { return e.message }

type tokenEntry struct {
	value   string
	expires time.Time
}
type Sender struct {
	Store      *Store
	Policy     Policy
	Scripts    *ScriptPool
	clients    sync.Map
	tokenMu    sync.Mutex
	tokenLocks sync.Map
	tokens     map[string]tokenEntry
}

func NewSender(s *Store, p Policy, scripts *ScriptPool) *Sender {
	return &Sender{Store: s, Policy: p, Scripts: scripts, tokens: map[string]tokenEntry{}}
}
func render(text string, data any) (string, error) {
	t, e := template.New("value").Option("missingkey=error").Funcs(template.FuncMap{"json": func(v any) (string, error) { b, e := json.Marshal(v); return string(b), e }, "get": func(v map[string]any, k string) any { return v[k] }, "queryEscape": url.QueryEscape, "pathEscape": url.PathEscape, "base64": func(v string) string { return base64.StdEncoding.EncodeToString([]byte(v)) }, "xmlEscape": func(v string) string { var b bytes.Buffer; _ = xml.EscapeText(&b, []byte(v)); return b.String() }}).Parse(text)
	if e != nil {
		return "", e
	}
	var b limitedBuffer
	b.limit = 1048576
	if e = t.Execute(&b, data); e != nil {
		return "", e
	}
	return b.String(), nil
}

type limitedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.limit {
		return 0, errors.New("output too large")
	}
	return b.Buffer.Write(p)
}
func (s *Sender) Build(ctx context.Context, d Delivery) (BuiltRequest, error) {
	ep := d.Endpoint
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := ID()
	var obj any
	dec := json.NewDecoder(bytes.NewReader(d.Event.Raw))
	dec.UseNumber()
	_ = dec.Decode(&obj)
	data := map[string]any{"JSON": obj, "Raw": string(d.Event.Raw), "Metadata": d.Event.Metadata, "DeliveryID": d.ID, "Timestamp": stamp, "Nonce": nonce}
	r := BuiltRequest{Method: ep.Method, URL: ep.URL, Headers: map[string]string{}, Body: string(d.Event.Raw)}
	var e error
	if ep.BodyMode == "connector" {
		if s.Store == nil {
			return r, errors.New("connector preview is disabled to preserve offline preview")
		}
		r, e = s.connector(ctx, d, stamp, nonce)
		if e != nil {
			return r, e
		}
	} else if ep.BodyMode == "script" {
		if s.Scripts == nil {
			return r, errors.New("trusted scripts are not enabled")
		}
		secret := ""
		if ep.Auth.Type == "script" && s.Store != nil {
			cred, err := s.Store.Credential(ctx, ep.Auth.CredentialRef)
			if err != nil {
				return r, err
			}
			secret = cred["secret"]
		} else if ep.Auth.Type == "script" {
			secret = "preview-only-not-a-real-secret"
		}
		v, err := s.Scripts.Run(ctx, ScriptJob{Script: ep.Script, Function: "buildRequest", Secret: secret, Context: map[string]any{"event": map[string]any{"json": safeNumbers(obj), "raw": string(d.Event.Raw), "raw_base64": base64.StdEncoding.EncodeToString(d.Event.Raw), "metadata": d.Event.Metadata}, "delivery_id": d.ID, "timestamp": stamp, "nonce": nonce}})
		if err != nil {
			return r, err
		}
		b, _ := json.Marshal(v)
		if err = json.Unmarshal(b, &r); err != nil {
			return r, err
		}
		if r.BodyBase64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(r.BodyBase64)
			if err != nil || len(decoded) > 1048576 {
				return r, errors.New("invalid base64 request body")
			}
			r.Body = string(decoded)
		}
	} else {
		if r.URL, e = render(ep.URL, data); e != nil {
			return r, e
		}
		if ep.BodyMode == "template" {
			if r.Body, e = render(ep.Body, data); e != nil {
				return r, e
			}
		}
		for k, v := range ep.Headers {
			if r.Headers[k], e = render(v, data); e != nil {
				return r, e
			}
		}
	}
	if r.Headers == nil {
		r.Headers = map[string]string{}
	}
	if r.BodyBase64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(r.BodyBase64)
		if err != nil || len(decoded) > 1048576 {
			return r, errors.New("invalid base64 request body")
		}
		r.Body = string(decoded)
	}
	if ep.IdempotencyHeader != "" {
		r.Headers[ep.IdempotencyHeader] = d.IdempotencyKey
	}
	u, e := url.Parse(r.URL)
	if e != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return r, errors.New("invalid request URL")
	}
	// Dynamic URL must stay on the configured origin, including port and scheme.
	base, _ := url.Parse(ep.URL)
	if !strings.EqualFold(base.Host, u.Host) || base.Scheme != u.Scheme {
		return r, errors.New("script cannot change configured target origin")
	}
	for k, v := range r.Headers {
		if strings.ContainsAny(k+v, "\r\n") {
			return r, errors.New("invalid request header")
		}
	}
	auths := append([]Auth{ep.Auth}, ep.AuthChain...)
	for _, auth := range auths {
		ep.Auth = auth
		if auth.Type == "" || auth.Type == "none" || auth.Type == "script" {
			continue
		}
		if s.Store == nil {
			return r, errors.New("credentials unavailable")
		}
		cred, err := s.Store.Credential(ctx, ep.Auth.CredentialRef)
		if err != nil {
			return r, err
		}
		switch ep.Auth.Type {
		case "bearer":
			r.Headers["Authorization"] = "Bearer " + cred["token"]
		case "basic":
			r.Headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(cred["username"]+":"+cred["password"]))
		case "api_key":
			header := ep.Auth.Header
			if header == "" {
				header = "X-API-Key"
			}
			if ep.Auth.Location == "query" {
				q := u.Query()
				q.Set(header, cred["secret"])
				u.RawQuery = q.Encode()
				r.URL = u.String()
			} else {
				r.Headers[header] = cred["secret"]
			}
		case "hmac_sha256", "rsa_sha256":
			if ep.Auth.Location == "query" {
				q := u.Query()
				field := ep.Auth.Header
				if field == "" {
					field = "X-Signature"
				}
				q.Del(field)
				if ep.Auth.TimestampField != "" {
					q.Set(ep.Auth.TimestampField, stamp)
				}
				u.RawQuery = q.Encode()
				r.URL = u.String()
			}
			hash := sha256.Sum256([]byte(r.Body))
			canonical := ep.Auth.Canonical
			if canonical == "" {
				canonical = "{{.Timestamp}}\n{{.BodySHA256}}"
			}
			msg, err := render(canonical, map[string]any{"Timestamp": stamp, "Nonce": nonce, "Body": r.Body, "BodySHA256": hex.EncodeToString(hash[:]), "Method": r.Method, "Path": u.EscapedPath(), "Query": u.RawQuery, "Headers": r.Headers})
			if err != nil {
				return r, err
			}
			var sig []byte
			if ep.Auth.Type == "hmac_sha256" {
				h := hmac.New(sha256.New, []byte(cred["secret"]))
				h.Write([]byte(msg))
				sig = h.Sum(nil)
			} else {
				block, _ := pem.Decode([]byte(cred["private_key"]))
				if block == nil {
					return r, errors.New("invalid RSA key")
				}
				key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
				if err != nil {
					k, err2 := x509.ParsePKCS1PrivateKey(block.Bytes)
					if err2 != nil {
						return r, errors.New("invalid RSA key")
					}
					key = k
				}
				rsaKey, ok := key.(*rsa.PrivateKey)
				if !ok {
					return r, errors.New("RSA key required")
				}
				h := sha256.Sum256([]byte(msg))
				sig, err = rsa.SignPKCS1v15(nil, rsaKey, crypto.SHA256, h[:])
				if err != nil {
					return r, errors.New("RSA signing failed")
				}
			}
			encoded := hex.EncodeToString(sig)
			if ep.Auth.Encoding == "base64" {
				encoded = base64.StdEncoding.EncodeToString(sig)
			}
			header := ep.Auth.Header
			if header == "" {
				header = "X-Signature"
			}
			if ep.Auth.Location == "query" {
				q := u.Query()
				q.Set(header, encoded)
				if ep.Auth.TimestampField != "" {
					q.Set(ep.Auth.TimestampField, stamp)
				}
				u.RawQuery = q.Encode()
				r.URL = u.String()
			} else {
				r.Headers[header] = encoded
				ts := ep.Auth.TimestampField
				if ts == "" {
					ts = "X-Timestamp"
				}
				r.Headers[ts] = stamp
			}
		case "oauth2":
			tok, err := s.token(ctx, ep, cred)
			if err != nil {
				return r, err
			}
			r.Headers["Authorization"] = "Bearer " + tok
		}
	}
	return r, nil
}

func (s *Sender) connector(ctx context.Context, d Delivery, stamp, nonce string) (BuiltRequest, error) {
	var result BuiltRequest
	b, e := json.Marshal(map[string]any{"version": 1, "delivery_id": d.ID, "idempotency_key": d.IdempotencyKey, "timestamp": stamp, "nonce": nonce, "event": d.Event, "target": map[string]any{"url": d.Endpoint.URL, "method": d.Endpoint.Method, "headers": d.Endpoint.Headers}})
	if e != nil {
		return result, e
	}
	req, e := http.NewRequestWithContext(ctx, "POST", d.Endpoint.ConnectorURL, bytes.NewReader(b))
	if e != nil {
		return result, errors.New("invalid connector request")
	}
	req.Header.Set("Content-Type", "application/json")
	if d.Endpoint.ConnectorCredentialRef != "" {
		c, e := s.Store.Credential(ctx, d.Endpoint.ConnectorCredentialRef)
		if e != nil {
			return result, e
		}
		req.Header.Set("Authorization", "Bearer "+c["token"])
	}
	client, e := s.client(ctx, d.Endpoint)
	if e != nil {
		return result, e
	}
	resp, e := client.Do(req)
	if e != nil {
		return result, &temporaryError{"connector unavailable"}
	}
	defer resp.Body.Close()
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return result, &temporaryError{"connector temporarily unavailable"}
	}
	if resp.StatusCode != 200 {
		return result, errors.New("connector rejected request")
	}
	body, e := io.ReadAll(io.LimitReader(resp.Body, 1048577))
	if e != nil || len(body) > 1048576 {
		return result, errors.New("connector response too large")
	}
	if e = json.Unmarshal(body, &result); e != nil {
		return result, errors.New("invalid connector response")
	}
	return result, nil
}
func (s *Sender) client(ctx context.Context, ep Endpoint) (*http.Client, error) {
	key := strconv.FormatBool(ep.AllowPrivate) + ":" + ep.TLSCredentialRef
	if c, ok := s.clients.Load(key); ok {
		return c.(*http.Client), nil
	}
	tr := &http.Transport{Proxy: nil, DialContext: s.Policy.Dial(ep.AllowPrivate), ForceAttemptHTTP2: true, MaxIdleConns: 256, MaxIdleConnsPerHost: 32, MaxConnsPerHost: 256, IdleConnTimeout: 60 * time.Second, TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 5 * time.Second, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	if ep.TLSCredentialRef != "" {
		c, e := s.Store.Credential(ctx, ep.TLSCredentialRef)
		if e != nil {
			return nil, e
		}
		if c["ca_pem"] != "" {
			roots, e := x509.SystemCertPool()
			if e != nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM([]byte(c["ca_pem"])) {
				return nil, errors.New("invalid CA certificate")
			}
			tr.TLSClientConfig.RootCAs = roots
		}
		if c["certificate_pem"] != "" {
			cert, e := tls.X509KeyPair([]byte(c["certificate_pem"]), []byte(c["private_key"]))
			if e != nil {
				return nil, errors.New("invalid client certificate")
			}
			tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
		}
	}
	c := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	actual, loaded := s.clients.LoadOrStore(key, c)
	if loaded {
		tr.CloseIdleConnections()
	}
	return actual.(*http.Client), nil
}
func (s *Sender) token(ctx context.Context, ep Endpoint, c map[string]string) (string, error) {
	l, _ := s.tokenLocks.LoadOrStore(ep.Auth.CredentialRef, &sync.Mutex{})
	lock := l.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()
	s.tokenMu.Lock()
	t, ok := s.tokens[ep.Auth.CredentialRef]
	s.tokenMu.Unlock()
	if ok && time.Until(t.expires) > 30*time.Second {
		return t.value, nil
	}
	u, e := url.Parse(c["token_url"])
	if e != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return "", errors.New("invalid token URL")
	}
	form := url.Values{"grant_type": {"client_credentials"}, "scope": {c["scope"]}}
	req, e := http.NewRequestWithContext(ctx, "POST", u.String(), strings.NewReader(form.Encode()))
	if e != nil {
		return "", errors.New("invalid token request")
	}
	req.SetBasicAuth(c["client_id"], c["client_secret"])
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client, e := s.client(ctx, ep)
	if e != nil {
		return "", e
	}
	resp, e := client.Do(req)
	if e != nil {
		return "", &temporaryError{"token service unavailable"}
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			return "", &temporaryError{"token service temporarily unavailable"}
		}
		return "", errors.New("token service rejected request")
	}
	var v struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if e = json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&v); e != nil || v.AccessToken == "" {
		return "", errors.New("invalid token response")
	}
	if v.ExpiresIn <= 0 || v.ExpiresIn > 86400 {
		v.ExpiresIn = 300
	}
	s.tokenMu.Lock()
	s.tokens[ep.Auth.CredentialRef] = tokenEntry{v.AccessToken, time.Now().Add(time.Duration(v.ExpiresIn) * time.Second)}
	s.tokenMu.Unlock()
	return v.AccessToken, nil
}
func (s *Sender) Send(parent context.Context, d Delivery) Result {
	start := time.Now()
	r := Result{Status: "DEAD"}
	ctx, cancel := context.WithTimeout(parent, time.Duration(d.Endpoint.TimeoutMS)*time.Millisecond)
	defer cancel()
	built, e := s.Build(ctx, d)
	if e != nil {
		r.Error = "request construction failed"
		var temporary *temporaryError
		if errors.As(e, &temporary) {
			r.Status = "RETRY_WAIT"
			r.Error = temporary.message
		}
		return r
	}
	req, e := http.NewRequestWithContext(ctx, built.Method, built.URL, strings.NewReader(built.Body))
	if e != nil {
		r.Error = "invalid request"
		return r
	}
	for k, v := range built.Headers {
		req.Header.Set(k, v)
	}
	client, e := s.client(ctx, d.Endpoint)
	if e != nil {
		r.Error = "TLS credential configuration failed"
		return r
	}
	resp, e := client.Do(req)
	r.Duration = time.Since(start)
	if e != nil {
		r.Status = "RETRY_WAIT"
		r.Error = "network failure; remote outcome may be unknown"
		return r
	}
	defer resp.Body.Close()
	r.HTTPStatus = resp.StatusCode
	if resp.StatusCode == 401 && d.Endpoint.Auth.Type == "oauth2" && d.Attempts <= 1 {
		s.tokenMu.Lock()
		delete(s.tokens, d.Endpoint.Auth.CredentialRef)
		s.tokenMu.Unlock()
		r.Status = "RETRY_WAIT"
		r.Error = "OAuth token rejected; refresh scheduled"
		return r
	}
	body, e := io.ReadAll(io.LimitReader(resp.Body, 262145))
	if e != nil {
		r.Status = "RETRY_WAIT"
		r.Error = "response read failed"
		return r
	}
	if len(body) > 262144 {
		r.Error = "response exceeds 256 KiB"
		return r
	}
	r.Duration = time.Since(start)
	if d.Endpoint.ResponseScript != "" {
		if s.Scripts == nil {
			r.Error = "response scripts disabled"
			return r
		}
		value, e := s.Scripts.Run(ctx, ScriptJob{Script: d.Endpoint.ResponseScript, Function: "classifyResponse", Context: map[string]any{"status": resp.StatusCode, "headers": resp.Header, "body": string(body)}})
		if e != nil {
			r.Error = "response script failed"
			return r
		}
		switch value {
		case "success":
			r.Status = "SUCCEEDED"
		case "retry":
			r.Status = "RETRY_WAIT"
			r.Error = "response script requested retry"
		case "dead":
			r.Error = "response script rejected response"
		default:
			r.Error = "response script must return success, retry or dead"
		}
		return r
	}
	if resp.StatusCode == 429 || resp.StatusCode == 408 || resp.StatusCode >= 500 {
		r.Status = "RETRY_WAIT"
		r.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		if n, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && n > 0 {
			r.RetryAfter = time.Duration(min(n, 3600)) * time.Second
		} else if t, e := http.ParseTime(resp.Header.Get("Retry-After")); e == nil {
			r.RetryAfter = time.Until(t)
		}
		return r
	}
	ok := false
	for _, code := range d.Endpoint.SuccessStatuses {
		if resp.StatusCode == code {
			ok = true
		}
	}
	if !ok {
		r.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return r
	}
	if d.Endpoint.SuccessPath != "" {
		v, e := Extract(body, d.Endpoint.SuccessPath)
		if e != nil {
			r.Error = "response success path missing"
			return r
		}
		b, _ := json.Marshal(v)
		var want any
		dec := json.NewDecoder(bytes.NewReader(d.Endpoint.SuccessValue))
		dec.UseNumber()
		if dec.Decode(&want) != nil {
			r.Error = "invalid response success value"
			return r
		}
		w, _ := json.Marshal(want)
		if string(b) != string(w) {
			r.Error = "response business condition failed"
			return r
		}
	}
	r.Status = "SUCCEEDED"
	return r
}
func safeNumbers(v any) any {
	switch x := v.(type) {
	case json.Number:
		return x.String()
	case map[string]any:
		for k, v := range x {
			x[k] = safeNumbers(v)
		}
	case []any:
		for i, v := range x {
			x[i] = safeNumbers(v)
		}
	}
	return v
}
