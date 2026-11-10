package relay

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"golang.org/x/time/rate"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

type SourceStatus struct {
	State     string    `json:"state"`
	Message   string    `json:"message"`
	UpdatedAt time.Time `json:"updated_at"`
}
type sourceRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	config string
}
type Sources struct {
	Store  *Store
	mu     sync.Mutex
	states map[string]SourceStatus
}

func NewSources(s *Store) *Sources { return &Sources{Store: s, states: map[string]SourceStatus{}} }
func (m *Sources) set(id, state, msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[id] = SourceStatus{state, msg, time.Now()}
}
func (m *Sources) Status() map[string]SourceStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]SourceStatus{}
	for k, v := range m.states {
		out[k] = v
	}
	return out
}
func (m *Sources) Run(ctx context.Context) {
	running := map[string]sourceRun{}
	defer func() {
		for _, r := range running {
			r.cancel()
			<-r.done
		}
	}()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			list, e := m.Store.List(ctx, "sources")
			if e != nil {
				continue
			}
			wanted := map[string]string{}
			for _, b := range list {
				var s Source
				if json.Unmarshal(b, &s) != nil {
					continue
				}
				if !s.Enabled {
					m.set(s.ID, "PAUSED", "")
					continue
				}
				if s.Type == "manual" {
					m.set(s.ID, "RUNNING", "manual admission enabled")
					continue
				}
				wanted[s.ID] = string(b)
				if old, ok := running[s.ID]; ok && old.config != string(b) {
					old.cancel()
					<-old.done
					delete(running, s.ID)
				}
				if _, ok := running[s.ID]; !ok {
					child, cancel := context.WithCancel(ctx)
					done := make(chan struct{})
					running[s.ID] = sourceRun{cancel, done, string(b)}
					go func(s Source) {
						defer close(done)
						backoff := time.Second
						for child.Err() == nil {
							m.set(s.ID, "CONNECTING", "")
							var err error
							if s.Type == "kafka" {
								err = m.kafka(child, s)
							} else {
								err = m.rabbit(child, s)
							}
							if child.Err() != nil {
								return
							}
							if err != nil {
								m.set(s.ID, "ERROR", "source disconnected or admission failed; inspect configuration and storage")
							}
							select {
							case <-child.Done():
								return
							case <-time.After(backoff):
							}
							backoff = min(backoff*2, 30*time.Second)
						}
					}(s)
				}
			}
			for id, r := range running {
				if _, ok := wanted[id]; !ok {
					r.cancel()
					<-r.done
					delete(running, id)
				}
			}
		}
	}
}
func (m *Sources) kafka(ctx context.Context, s Source) error {
	budget := rate.NewLimiter(rate.Limit(s.ReadBytesPerSecond), 1048576)
	offset := kgo.NewOffset().AtEnd()
	if s.Start == "earliest" {
		offset = kgo.NewOffset().AtStart()
	}
	opts := []kgo.Opt{kgo.SeedBrokers(s.Brokers...), kgo.ConsumerGroup(s.Group), kgo.ConsumeTopics(s.Topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeStartOffset(offset), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.FetchMaxBytes(2 * 1048576), kgo.FetchMaxPartitionBytes(1048576), kgo.MaxBufferedRecords(200), kgo.FetchIsolationLevel(kgo.ReadCommitted())}
	if s.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	if s.CredentialRef != "" {
		c, e := m.Store.Credential(ctx, s.CredentialRef)
		if e != nil {
			return e
		}
		switch c["mechanism"] {
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: c["username"], Pass: c["password"]}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: c["username"], Pass: c["password"]}.AsSha512Mechanism()))
		default:
			opts = append(opts, kgo.SASL(plain.Auth{User: c["username"], Pass: c["password"]}.AsMechanism()))
		}
	}
	client, e := kgo.NewClient(opts...)
	if e != nil {
		return e
	}
	defer client.CloseAllowingRebalance()
	if e = client.Ping(ctx); e != nil {
		return e
	}
	m.set(s.ID, "RUNNING", "")
	for ctx.Err() == nil {
		fetch := client.PollRecords(ctx, 200)
		if errs := fetch.Errors(); len(errs) > 0 {
			client.AllowRebalance()
			return errors.New("Kafka fetch failed; offsets must not silently reset")
		}
		records := fetch.Records()
		for _, r := range records {
			if len(r.Value) > 1048576 {
				return errors.New("message exceeds configured limit")
			}
			if e = budget.WaitN(ctx, len(r.Value)); e != nil {
				return e
			}
			hs := []map[string]string{}
			for _, h := range r.Headers {
				hs = append(hs, map[string]string{"name": h.Key, "value_base64": base64.StdEncoding.EncodeToString(h.Value)})
			}
			meta := map[string]any{"topic": r.Topic, "partition": r.Partition, "offset": fmt.Sprint(r.Offset), "key_base64": base64.StdEncoding.EncodeToString(r.Key), "headers": hs}
			if _, e = m.Store.Admit(ctx, s.ID, fmt.Sprintf("%s:%d:%d", r.Topic, r.Partition, r.Offset), r.Value, meta); e != nil {
				client.AllowRebalance()
				return e
			}
		}
		if len(records) > 0 {
			if e = client.CommitRecords(ctx, records...); e != nil {
				client.AllowRebalance()
				return e
			}
		}
		client.AllowRebalance()
	}
	return ctx.Err()
}
func (m *Sources) rabbit(ctx context.Context, s Source) error {
	budget := rate.NewLimiter(rate.Limit(s.ReadBytesPerSecond), 1048576)
	u, e := url.Parse(s.URL)
	if e != nil {
		return e
	}
	if s.CredentialRef != "" {
		c, e := m.Store.Credential(ctx, s.CredentialRef)
		if e != nil {
			return e
		}
		u.User = url.UserPassword(c["username"], c["password"])
	}
	conn, e := amqp.DialConfig(u.String(), amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, address string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
	}})
	if e != nil {
		return errors.New("RabbitMQ connection failed")
	}
	defer conn.Close()
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-conn.NotifyClose(make(chan *amqp.Error, 1)):
		}
	}()
	ch, e := conn.Channel()
	if e != nil {
		return e
	}
	defer ch.Close()
	if e = ch.Qos(32, 0, false); e != nil {
		return e
	}
	if s.Declare {
		if _, e = ch.QueueDeclare(s.Queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum", "x-max-length-bytes": s.QueueMaxBytes, "x-overflow": s.Overflow}); e != nil {
			return e
		}
		if e = ch.QueueBind(s.Queue, s.RoutingKey, s.Exchange, false, nil); e != nil {
			return e
		}
	} else {
		if _, e = ch.QueueInspect(s.Queue); e != nil {
			return e
		}
	}
	messages, e := ch.Consume(s.Queue, "relay-"+ID(), false, false, false, false, nil)
	if e != nil {
		return e
	}
	m.set(s.ID, "RUNNING", "")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return errors.New("RabbitMQ channel closed")
			}
			if len(msg.Body) > 1048576 {
				return errors.New("message exceeds configured limit")
			}
			if e = budget.WaitN(ctx, len(msg.Body)); e != nil {
				return e
			}
			meta := map[string]any{"exchange": msg.Exchange, "routing_key": msg.RoutingKey, "message_id": msg.MessageId, "content_type": msg.ContentType, "redelivered": msg.Redelivered, "headers": msg.Headers}
			_, e = m.Store.Admit(ctx, s.ID, "", msg.Body, meta)
			if e != nil {
				return e
			}
			if e = msg.Ack(false); e != nil {
				return e
			}
		}
	}
}
func ValidateSourceMutation(old, next Source) error {
	a, b := old, next
	a.Enabled = false
	b.Enabled = false
	a.Name = ""
	b.Name = ""
	ba, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	if strings.Compare(string(ba), string(bb)) != 0 {
		return errors.New("source connection/epoch is immutable: create a new source to change subscription")
	}
	return nil
}
