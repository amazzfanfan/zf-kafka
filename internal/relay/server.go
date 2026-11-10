package relay

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

//go:embed all:public
var public embed.FS

type session struct{ expires time.Time }
type Server struct {
	Store          *Store
	Sources        *Sources
	Sender         *Sender
	Prefix, Token  string
	TrustedScripts bool
	mu             sync.Mutex
	sessions       map[string]session
	loginLimit     *rate.Limiter
}

func (s *Server) Handler() http.Handler {
	s.sessions = map[string]session{}
	s.loginLimit = rate.NewLimiter(rate.Limit(0.5), 10)
	api := "/api/" + s.Prefix + "/v1"
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/"+s.Prefix+"/metrics", func(w http.ResponseWriter, r *http.Request) {
		stats, e := s.Store.Stats(r.Context())
		if e != nil {
			fail(w, 503, "storage unavailable")
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintln(w, "# HELP relay_deliveries Current delivery tasks by state.")
		fmt.Fprintln(w, "# TYPE relay_deliveries gauge")
		for status, n := range stats {
			fmt.Fprintf(w, "relay_deliveries{status=%q} %d\n", status, n)
		}
	})
	mux.HandleFunc("POST "+api+"/sessions", s.login)
	mux.HandleFunc("DELETE "+api+"/sessions", s.logout)
	mux.HandleFunc("GET "+api+"/health/live", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]any{"status": "alive"}) })
	mux.HandleFunc("GET "+api+"/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if s.Store.DB.Ping(ctx) != nil {
			fail(w, 503, "storage unavailable")
			return
		}
		writeJSON(w, 200, map[string]any{"status": "ready"})
	})
	mux.HandleFunc("GET "+api+"/me", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"role": "admin", "trusted_scripts": s.TrustedScripts})
	})
	mux.HandleFunc("GET "+api+"/stats", func(w http.ResponseWriter, r *http.Request) { v, e := s.Store.Stats(r.Context()); respond(w, v, e) })
	mux.HandleFunc("GET "+api+"/source-status", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.Sources.Status()) })
	for _, kind := range []string{"sources", "endpoints", "routes"} {
		kind := kind
		mux.HandleFunc("GET "+api+"/"+kind, func(w http.ResponseWriter, r *http.Request) {
			v, e := s.Store.List(r.Context(), kind)
			respond(w, v, e)
		})
		mux.HandleFunc("POST "+api+"/"+kind, func(w http.ResponseWriter, r *http.Request) { s.saveConfig(w, r, kind, "") })
		mux.HandleFunc("PUT "+api+"/"+kind+"/{id}", func(w http.ResponseWriter, r *http.Request) { s.saveConfig(w, r, kind, r.PathValue("id")) })
	}
	mux.HandleFunc("GET "+api+"/credentials", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.Store.Credentials(r.Context())
		respond(w, v, e)
	})
	mux.HandleFunc("POST "+api+"/credentials", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Name string            `json:"name"`
			Data map[string]string `json:"data"`
		}
		if decode(w, r, &v) != nil {
			return
		}
		if v.Name == "" || len(v.Data) == 0 {
			fail(w, 400, "credential name/data required")
			return
		}
		list, e := s.Store.Credentials(r.Context())
		if e != nil {
			fail(w, 500, "storage failure")
			return
		}
		if len(list) >= 128 {
			fail(w, 400, "credential limit 128 reached")
			return
		}
		id, e := s.Store.PutCredential(r.Context(), v.Name, v.Data)
		respond(w, map[string]string{"id": id}, e)
	})
	mux.HandleFunc("GET "+api+"/deliveries", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.Store.Deliveries(r.Context(), r.URL.Query().Get("before"))
		respond(w, v, e)
	})
	mux.HandleFunc("GET "+api+"/deliveries/{id}/attempts", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.Store.Attempts(r.Context(), r.PathValue("id"))
		respond(w, v, e)
	})
	mux.HandleFunc("POST "+api+"/deliveries/{id}/replay", func(w http.ResponseWriter, r *http.Request) {
		id, e := s.Store.Replay(r.Context(), r.PathValue("id"))
		if e != nil {
			fail(w, 409, "only completed deliveries may be replayed")
			return
		}
		writeJSON(w, 201, map[string]string{"id": id})
	})
	mux.HandleFunc("POST "+api+"/deliveries/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		tag, e := s.Store.DB.Exec(r.Context(), "UPDATE deliveries SET status='CANCELLED',completed_at=now() WHERE id=$1 AND status IN ('PENDING','RETRY_WAIT')", r.PathValue("id"))
		if e != nil {
			fail(w, 500, "storage failure")
			return
		}
		if tag.RowsAffected() == 0 {
			fail(w, 409, "only waiting deliveries can be cancelled")
			return
		}
		_, _ = s.Store.DB.Exec(r.Context(), "INSERT INTO audit(action,object_id) VALUES('cancel',$1)", r.PathValue("id"))
		writeJSON(w, 200, map[string]bool{"cancelled": true})
	})
	mux.HandleFunc("POST "+api+"/sources/{id}/messages", func(w http.ResponseWriter, r *http.Request) {
		var src Source
		if s.Store.Get(r.Context(), "sources", r.PathValue("id"), &src) != nil || src.Type != "manual" {
			fail(w, 400, "manual source required")
			return
		}
		var v struct {
			Body     string `json:"body"`
			DedupKey string `json:"dedup_key"`
		}
		if decode(w, r, &v) != nil {
			return
		}
		id, e := s.Store.Admit(r.Context(), src.ID, v.DedupKey, []byte(v.Body), map[string]any{"type": "manual"})
		respond(w, map[string]string{"event_id": id}, e)
	})
	mux.HandleFunc("POST "+api+"/debug/preview", s.preview)
	mux.HandleFunc("GET "+api+"/audit", func(w http.ResponseWriter, r *http.Request) {
		rows, e := s.Store.DB.Query(r.Context(), "SELECT action,object_id,created_at FROM audit ORDER BY id DESC LIMIT 100")
		if e != nil {
			fail(w, 500, "storage failure")
			return
		}
		defer rows.Close()
		out := []map[string]any{}
		for rows.Next() {
			var a, id string
			var t time.Time
			if rows.Scan(&a, &id, &t) != nil {
				fail(w, 500, "storage failure")
				return
			}
			out = append(out, map[string]any{"action": a, "object_id": id, "created_at": t})
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc(api+"/", func(w http.ResponseWriter, r *http.Request) { fail(w, 404, "API not found") })
	sub, _ := fs.Sub(public, "public")
	files := http.FileServer(http.FS(sub))
	root := "/" + s.Prefix + "/"
	mux.HandleFunc("GET "+root, func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, root)
		if rel == "runtime-config.json" {
			w.Header().Set("Cache-Control", "no-store")
			writeJSON(w, 200, map[string]string{"prefix": s.Prefix, "api_base": api})
			return
		}
		if rel != "" {
			if f, e := sub.Open(path.Clean(rel)); e == nil {
				f.Close()
				http.StripPrefix(strings.TrimSuffix(root, "/"), files).ServeHTTP(w, r)
				return
			}
			if strings.Contains(rel, ".") || strings.HasPrefix(rel, "assets/") {
				http.NotFound(w, r)
				return
			}
			known := map[string]bool{"overview": true, "sources": true, "endpoints": true, "routes": true, "deliveries": true, "debug": true, "credentials": true, "audit": true}
			if !known[strings.Split(rel, "/")[0]] {
				http.NotFound(w, r)
				return
			}
		}
		b, e := fs.ReadFile(sub, "index.html")
		if e != nil {
			http.Error(w, "build frontend first", 503)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Write([]byte(strings.Replace(string(b), "<head>", "<head><base href=\"/"+s.Prefix+"/\">", 1)))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, root, 302)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
			publicPath := r.URL.Path == api+"/sessions" && r.Method == "POST" || r.URL.Path == api+"/health/live" || r.URL.Path == api+"/health/ready"
			if !publicPath && !s.authorize(w, r) {
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}
func (s *Server) authorize(w http.ResponseWriter, r *http.Request) bool {
	bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if bearer != "" && secureEqual(bearer, s.Token) {
		return true
	}
	cookie, e := r.Cookie("relay_" + s.Prefix)
	if e != nil {
		fail(w, 401, "authentication required")
		return false
	}
	s.mu.Lock()
	v, ok := s.sessions[cookie.Value]
	if ok && time.Now().After(v.expires) {
		delete(s.sessions, cookie.Value)
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		fail(w, 401, "session expired")
		return false
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		origin, e := url.Parse(r.Header.Get("Origin"))
		if e != nil || origin.Host != r.Host || (origin.Scheme != "https" && origin.Scheme != "http") {
			fail(w, 403, "same-origin request required")
			return false
		}
	}
	return true
}
func secureEqual(a, b string) bool {
	ah, bh := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ah[:], bh[:]) == 1
}
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if !s.loginLimit.Allow() {
		fail(w, 429, "too many login attempts")
		return
	}
	var v struct {
		Token string `json:"token"`
	}
	if decode(w, r, &v) != nil {
		return
	}
	if !secureEqual(v.Token, s.Token) {
		fail(w, 401, "invalid token")
		return
	}
	id := ID() + ID()
	s.mu.Lock()
	for k, v := range s.sessions {
		if time.Now().After(v.expires) {
			delete(s.sessions, k)
		}
	}
	if len(s.sessions) >= 1000 {
		s.mu.Unlock()
		fail(w, 429, "session limit reached")
		return
	}
	s.sessions[id] = session{time.Now().Add(12 * time.Hour)}
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "relay_" + s.Prefix, Value: id, Path: "/api/" + s.Prefix + "/", HttpOnly: true, Secure: r.TLS != nil || os.Getenv("RELAY_SECURE_COOKIE") == "true", SameSite: http.SameSiteStrictMode, MaxAge: 43200})
	writeJSON(w, 200, map[string]bool{"authenticated": true})
}
func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, e := r.Cookie("relay_" + s.Prefix); e == nil {
		s.mu.Lock()
		delete(s.sessions, c.Value)
		s.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: "relay_" + s.Prefix, Path: "/api/" + s.Prefix + "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
	writeJSON(w, 200, map[string]bool{"authenticated": false})
}
func (s *Server) saveConfig(w http.ResponseWriter, r *http.Request, kind, id string) {
	if id == "" {
		list, e := s.Store.List(r.Context(), kind)
		if e != nil {
			fail(w, 500, "storage failure")
			return
		}
		if len(list) >= 128 {
			fail(w, 400, "configuration limit 128 reached")
			return
		}
		id = ID()
	}
	var value any
	switch kind {
	case "sources":
		var v Source
		if decode(w, r, &v) != nil {
			return
		}
		v.ID = id
		if r.Method == "PUT" {
			var old Source
			if s.Store.Get(r.Context(), kind, id, &old) != nil {
				fail(w, 404, "source not found")
				return
			}
			if e := ValidateSourceMutation(old, v); e != nil {
				fail(w, 400, e.Error())
				return
			}
		}
		if e := v.Validate(); e != nil {
			fail(w, 400, e.Error())
			return
		}
		if v.CredentialRef != "" {
			if _, e := s.Store.Credential(r.Context(), v.CredentialRef); e != nil {
				fail(w, 400, "credential not found")
				return
			}
		}
		value = v
	case "endpoints":
		var v Endpoint
		if decode(w, r, &v) != nil {
			return
		}
		v.ID = id
		if e := v.Validate(); e != nil {
			fail(w, 400, e.Error())
			return
		}
		if (v.Script != "" || v.ResponseScript != "") && !s.TrustedScripts {
			fail(w, 400, "scripts disabled: enable only for trusted administrators")
			return
		}
		if e := CheckScript(v.Script); e != nil {
			fail(w, 400, e.Error())
			return
		}
		if e := CheckScript(v.ResponseScript); e != nil {
			fail(w, 400, e.Error())
			return
		}
		if v.TLSCredentialRef != "" {
			if _, e := s.Store.Credential(r.Context(), v.TLSCredentialRef); e != nil {
				fail(w, 400, "TLS credential not found")
				return
			}
		}
		if v.Auth.CredentialRef != "" {
			if _, e := s.Store.Credential(r.Context(), v.Auth.CredentialRef); e != nil {
				fail(w, 400, "credential not found")
				return
			}
		}
		for _, a := range v.AuthChain {
			if _, e := s.Store.Credential(r.Context(), a.CredentialRef); e != nil {
				fail(w, 400, "auth chain credential not found")
				return
			}
		}
		if v.ConnectorCredentialRef != "" {
			if _, e := s.Store.Credential(r.Context(), v.ConnectorCredentialRef); e != nil {
				fail(w, 400, "connector credential not found")
				return
			}
		}
		value = v
	case "routes":
		var v Route
		if decode(w, r, &v) != nil {
			return
		}
		v.ID = id
		if v.Name == "" || len(v.EndpointIDs) == 0 || len(v.EndpointIDs) > 32 {
			fail(w, 400, "name and 1..32 endpoints required")
			return
		}
		var src Source
		if s.Store.Get(r.Context(), "sources", v.SourceID, &src) != nil {
			fail(w, 400, "source not found")
			return
		}
		seen := map[string]bool{}
		for _, eid := range v.EndpointIDs {
			var ep Endpoint
			if seen[eid] || s.Store.Get(r.Context(), "endpoints", eid, &ep) != nil {
				fail(w, 400, "duplicate or missing endpoint")
				return
			}
			seen[eid] = true
		}
		if v.FilterPath != "" && !json.Valid(v.FilterValue) {
			fail(w, 400, "filter_value must be valid JSON")
			return
		}
		value = v
	default:
		fail(w, 404, "unknown config kind")
		return
	}
	if e := s.Store.Save(r.Context(), kind, id, value); e != nil {
		fail(w, 500, "configuration storage failed")
		return
	}
	writeJSON(w, 200, value)
}
func (s *Server) preview(w http.ResponseWriter, r *http.Request) {
	var v struct {
		Endpoint Endpoint `json:"endpoint"`
		Body     string   `json:"body"`
	}
	if decode(w, r, &v) != nil {
		return
	}
	if e := v.Endpoint.Validate(); e != nil {
		fail(w, 400, e.Error())
		return
	}
	if v.Endpoint.Script != "" && !s.TrustedScripts {
		fail(w, 400, "scripts disabled")
		return
	}
	if v.Endpoint.Auth.Type != "script" {
		v.Endpoint.Auth = Auth{}
	}
	v.Endpoint.AuthChain = nil
	d := Delivery{ID: "preview", IdempotencyKey: "preview", Endpoint: v.Endpoint, Event: Event{Raw: []byte(v.Body), Metadata: map[string]any{}}}
	previewSender := NewSender(nil, s.Sender.Policy, s.Sender.Scripts)
	req, e := previewSender.Build(r.Context(), d)
	if e != nil {
		fail(w, 400, "request preview failed; check template or script")
		return
	}
	if req.BodyBase64 != "" {
		req.Body = ""
	}
	writeJSON(w, 200, req)
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2*1048576)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		fail(w, 400, "invalid JSON: "+e.Error())
		return e
	}
	if e := d.Decode(&struct{}{}); e != io.EOF {
		fail(w, 400, "one JSON object required")
		return errors.New("trailing content")
	}
	return nil
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"data": v})
}
func fail(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}
func respond(w http.ResponseWriter, v any, e error) {
	if e != nil {
		fail(w, 500, "storage operation failed")
		return
	}
	writeJSON(w, 200, v)
}
