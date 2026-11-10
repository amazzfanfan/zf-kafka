package relay

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

func ID() string {
	var b [16]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type Source struct {
	ID                 string   `json:"id"`
	Name               string   `json:"name"`
	Type               string   `json:"type"`
	Enabled            bool     `json:"enabled"`
	Brokers            []string `json:"brokers,omitempty"`
	Topics             []string `json:"topics,omitempty"`
	Group              string   `json:"group,omitempty"`
	Start              string   `json:"start,omitempty"`
	URL                string   `json:"url,omitempty"`
	Queue              string   `json:"queue,omitempty"`
	Exchange           string   `json:"exchange,omitempty"`
	RoutingKey         string   `json:"routing_key,omitempty"`
	Declare            bool     `json:"declare,omitempty"`
	CredentialRef      string   `json:"credential_ref,omitempty"`
	TLS                bool     `json:"tls,omitempty"`
	Epoch              string   `json:"epoch"`
	QueueMaxBytes      int64    `json:"queue_max_bytes,omitempty"`
	Overflow           string   `json:"overflow,omitempty"`
	ReadBytesPerSecond int      `json:"read_bytes_per_second,omitempty"`
}
type Auth struct {
	Type           string `json:"type"`
	CredentialRef  string `json:"credential_ref"`
	Header         string `json:"header,omitempty"`
	Canonical      string `json:"canonical,omitempty"`
	Encoding       string `json:"encoding,omitempty"`
	Location       string `json:"location,omitempty"`
	TimestampField string `json:"timestamp_field,omitempty"`
}
type Endpoint struct {
	ID                     string            `json:"id"`
	Name                   string            `json:"name"`
	Enabled                bool              `json:"enabled"`
	URL                    string            `json:"url"`
	Method                 string            `json:"method"`
	Headers                map[string]string `json:"headers"`
	BodyMode               string            `json:"body_mode"`
	Body                   string            `json:"body,omitempty"`
	Script                 string            `json:"script,omitempty"`
	ResponseScript         string            `json:"response_script,omitempty"`
	Auth                   Auth              `json:"auth"`
	AuthChain              []Auth            `json:"auth_chain,omitempty"`
	TimeoutMS              int               `json:"timeout_ms"`
	Concurrency            int               `json:"concurrency"`
	RPS                    float64           `json:"rps"`
	MaxAttempts            int               `json:"max_attempts"`
	SuccessStatuses        []int             `json:"success_statuses,omitempty"`
	SuccessPath            string            `json:"success_path,omitempty"`
	SuccessValue           json.RawMessage   `json:"success_value,omitempty"`
	IdempotencyHeader      string            `json:"idempotency_header,omitempty"`
	AllowPrivate           bool              `json:"allow_private,omitempty"`
	TLSCredentialRef       string            `json:"tls_credential_ref,omitempty"`
	ConnectorURL           string            `json:"connector_url,omitempty"`
	ConnectorCredentialRef string            `json:"connector_credential_ref,omitempty"`
}
type Route struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Enabled     bool            `json:"enabled"`
	SourceID    string          `json:"source_id"`
	EndpointIDs []string        `json:"endpoint_ids"`
	FilterPath  string          `json:"filter_path,omitempty"`
	FilterValue json.RawMessage `json:"filter_value,omitempty"`
}
type Event struct {
	ID         string         `json:"id"`
	SourceID   string         `json:"source_id"`
	Raw        []byte         `json:"raw"`
	Metadata   map[string]any `json:"metadata"`
	ReceivedAt time.Time      `json:"received_at"`
}
type Delivery struct {
	ID             string    `json:"id"`
	EventID        string    `json:"event_id"`
	SourceID       string    `json:"source_id"`
	EndpointID     string    `json:"endpoint_id"`
	RouteID        string    `json:"route_id"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	Error          string    `json:"error"`
	CreatedAt      time.Time `json:"created_at"`
	NextAt         time.Time `json:"next_at"`
	Endpoint       Endpoint  `json:"-"`
	Event          Event     `json:"-"`
	Lease          string    `json:"-"`
	IdempotencyKey string    `json:"idempotency_key"`
}
type Attempt struct {
	ID         string    `json:"id"`
	Number     int       `json:"number"`
	Status     string    `json:"status"`
	HTTPStatus int       `json:"http_status"`
	Error      string    `json:"error"`
	DurationMS int64     `json:"duration_ms"`
	CreatedAt  time.Time `json:"created_at"`
}
type Result struct {
	Status     string
	HTTPStatus int
	Error      string
	RetryAfter time.Duration
	Duration   time.Duration
}

var prefixRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func ValidatePrefix(s string) error {
	if !prefixRE.MatchString(s) || s == "api" {
		return errors.New("prefix must be a nonempty path segment other than api")
	}
	return nil
}
func (s *Source) Validate() error {
	if s.ReadBytesPerSecond == 0 {
		s.ReadBytesPerSecond = 10 * 1048576
	}
	if s.ReadBytesPerSecond < 1048576 || s.ReadBytesPerSecond > 1024*1048576 {
		return errors.New("read_bytes_per_second must be 1 MiB/s..1 GiB/s")
	}
	if s.Name == "" {
		return errors.New("name required")
	}
	if s.Epoch == "" {
		s.Epoch = ID()
	}
	switch s.Type {
	case "manual":
	case "kafka":
		if len(s.Brokers) == 0 || len(s.Topics) == 0 {
			return errors.New("brokers and topics required")
		}
		if !strings.HasPrefix(s.Group, "relay.") {
			return errors.New("Kafka group must start with relay.; business groups are forbidden")
		}
		if s.Start != "earliest" && s.Start != "latest" {
			return errors.New("start must be earliest or latest")
		}
	case "rabbitmq":
		u, e := url.Parse(s.URL)
		if e != nil || u.Host == "" || (u.Scheme != "amqp" && u.Scheme != "amqps") || u.User != nil {
			return errors.New("AMQP URL without credentials required; use credential_ref")
		}
		if !strings.HasPrefix(s.Queue, "relay.") {
			return errors.New("dedicated queue name must start with relay.")
		}
		if s.Declare && s.Exchange == "" {
			return errors.New("default exchange cannot mirror a business queue; configure a named exchange")
		}
		if s.Declare {
			if s.QueueMaxBytes == 0 {
				s.QueueMaxBytes = 256 * 1048576
			}
			if s.QueueMaxBytes < 1048576 {
				return errors.New("queue_max_bytes must be at least 1 MiB")
			}
			if s.Overflow != "drop-head" && s.Overflow != "reject-publish" {
				return errors.New("choose overflow explicitly: drop-head may lose relay copies; reject-publish may affect business publisher confirms")
			}
		}
	default:
		return errors.New("source type must be kafka, rabbitmq or manual")
	}
	return nil
}
func (e *Endpoint) Validate() error {
	if len(e.AuthChain) > 4 {
		return errors.New("at most four additional auth steps")
	}
	for _, a := range e.AuthChain {
		copy := *e
		copy.AuthChain = nil
		copy.Auth = a
		if err := copy.Validate(); err != nil {
			return err
		}
	}
	if e.Name == "" {
		return errors.New("name required")
	}
	u, err := url.Parse(e.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
		return errors.New("valid HTTP(S) URL without userinfo/fragment required")
	}
	if e.Method == "" {
		e.Method = "POST"
	}
	if !regexp.MustCompile(`^[A-Z]+$`).MatchString(e.Method) {
		return errors.New("invalid HTTP method")
	}
	if e.TimeoutMS == 0 {
		e.TimeoutMS = 10000
	}
	if e.Concurrency == 0 {
		e.Concurrency = 16
	}
	if e.RPS == 0 {
		e.RPS = 50
	}
	if e.MaxAttempts == 0 {
		e.MaxAttempts = 8
	}
	if e.TimeoutMS < 100 || e.TimeoutMS > 15000 || e.Concurrency < 1 || e.Concurrency > 256 || e.RPS <= 0 || e.RPS > 100000 || e.MaxAttempts < 1 || e.MaxAttempts > 20 {
		return errors.New("limits out of allowed range")
	}
	if len(e.Script) > 32768 || len(e.ResponseScript) > 32768 || len(e.Body) > 1048576 {
		return errors.New("template/script too large")
	}
	switch e.BodyMode {
	case "", "raw", "template", "script", "connector":
	default:
		return errors.New("body_mode must be raw, template, script or connector")
	}
	if e.BodyMode == "connector" {
		u, err := url.Parse(e.ConnectorURL)
		if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
			return errors.New("valid connector_url required")
		}
	}
	switch e.Auth.Type {
	case "", "none", "bearer", "basic", "api_key", "hmac_sha256", "rsa_sha256", "oauth2", "script":
	default:
		return errors.New("unsupported auth type")
	}
	if e.Auth.Type != "" && e.Auth.Type != "none" && e.Auth.CredentialRef == "" {
		return errors.New("auth credential_ref required")
	}
	if e.Auth.Location != "" && e.Auth.Location != "header" && e.Auth.Location != "query" {
		return errors.New("auth location must be header or query")
	}
	if e.Auth.Encoding != "" && e.Auth.Encoding != "hex" && e.Auth.Encoding != "base64" {
		return errors.New("signature encoding must be hex or base64")
	}
	if len(e.SuccessStatuses) == 0 {
		e.SuccessStatuses = []int{200, 201, 202, 204}
	}
	for k, v := range e.Headers {
		if strings.EqualFold(k, "Authorization") || strings.EqualFold(k, "Cookie") {
			return errors.New("use credential-based auth instead of secret headers")
		}
		if strings.ContainsAny(k+v, "\r\n") {
			return errors.New("invalid header")
		}
	}
	return nil
}
func Extract(raw []byte, path string) (any, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if e := dec.Decode(&v); e != nil {
		return nil, e
	}
	path = strings.TrimPrefix(path, "$.")
	for _, k := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("path %s is not an object", path)
		}
		v, ok = m[k]
		if !ok {
			return nil, fmt.Errorf("path %s missing", path)
		}
	}
	return v, nil
}
