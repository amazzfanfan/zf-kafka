package relay

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	mathrand "math/rand/v2"
	"sync"
	"time"
)

//go:embed schema.sql
var schema string

type Store struct {
	DB              *pgxpool.Pool
	seal            cipher.AEAD
	MaxStorageBytes int64
	capacityMu      sync.Mutex
	capacityAt      time.Time
	capacityErr     error
}

func OpenStore(ctx context.Context, dsn string, key []byte) (*Store, error) {
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		return nil, e
	}
	cfg.MaxConns = 16
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "5000"
	db, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	b, e := aes.NewCipher(key)
	if e != nil {
		db.Close()
		return nil, e
	}
	a, e := cipher.NewGCM(b)
	if e != nil {
		db.Close()
		return nil, e
	}
	s := &Store{DB: db, seal: a, MaxStorageBytes: 10 * 1024 * 1024 * 1024}
	if e = db.Ping(ctx); e != nil {
		db.Close()
		return nil, e
	}
	return s, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(731992)"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, schema); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) List(ctx context.Context, kind string) ([]json.RawMessage, error) {
	r, e := s.DB.Query(ctx, "SELECT data FROM configs WHERE kind=$1 ORDER BY updated_at DESC", kind)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	out := []json.RawMessage{}
	for r.Next() {
		var b []byte
		if e = r.Scan(&b); e != nil {
			return nil, e
		}
		out = append(out, json.RawMessage(b))
	}
	return out, r.Err()
}
func (s *Store) Get(ctx context.Context, kind, id string, dst any) error {
	var b []byte
	e := s.DB.QueryRow(ctx, "SELECT data FROM configs WHERE kind=$1 AND id=$2", kind, id).Scan(&b)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, dst)
}
func (s *Store) Save(ctx context.Context, kind, id string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	// This lock serializes publication with event admission, ensuring one complete configuration snapshot.
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(731993)"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO configs(kind,id,data) VALUES($1,$2,$3) ON CONFLICT(kind,id) DO UPDATE SET data=$3,revision=configs.revision+1,updated_at=now()", kind, id, b); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "INSERT INTO audit(action,object_id) VALUES($1,$2)", "save_"+kind, id); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) PutCredential(ctx context.Context, name string, data map[string]string) (string, error) {
	id := ID()
	plain, e := json.Marshal(data)
	if e != nil {
		return "", e
	}
	nonce := make([]byte, s.seal.NonceSize())
	if _, e = io.ReadFull(rand.Reader, nonce); e != nil {
		return "", e
	}
	sealed := s.seal.Seal(nonce, nonce, plain, []byte(id))
	_, e = s.DB.Exec(ctx, "INSERT INTO credentials(id,name,ciphertext) VALUES($1,$2,$3)", id, name, sealed)
	return id, e
}
func (s *Store) Credential(ctx context.Context, id string) (map[string]string, error) {
	var b []byte
	if e := s.DB.QueryRow(ctx, "SELECT ciphertext FROM credentials WHERE id=$1", id).Scan(&b); e != nil {
		return nil, errors.New("credential not found")
	}
	if len(b) < s.seal.NonceSize() {
		return nil, errors.New("invalid credential")
	}
	p, e := s.seal.Open(nil, b[:s.seal.NonceSize()], b[s.seal.NonceSize():], []byte(id))
	if e != nil {
		return nil, errors.New("cannot decrypt credential")
	}
	var m map[string]string
	e = json.Unmarshal(p, &m)
	return m, e
}
func (s *Store) Credentials(ctx context.Context) ([]map[string]any, error) {
	r, e := s.DB.Query(ctx, "SELECT id,name,created_at FROM credentials ORDER BY created_at DESC")
	if e != nil {
		return nil, e
	}
	defer r.Close()
	out := []map[string]any{}
	for r.Next() {
		var id, n string
		var t time.Time
		if e = r.Scan(&id, &n, &t); e != nil {
			return nil, e
		}
		out = append(out, map[string]any{"id": id, "name": n, "created_at": t})
	}
	return out, r.Err()
}
func (s *Store) Admit(ctx context.Context, sourceID, dedup string, raw []byte, meta map[string]any) (string, error) {
	if e := s.capacity(ctx); e != nil {
		return "", e
	}
	if len(raw) > 1048576 {
		return "", errors.New("message exceeds 1 MiB; source must pause")
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(731993)"); e != nil {
		return "", e
	}
	var src Source
	var srcb []byte
	if e = tx.QueryRow(ctx, "SELECT data FROM configs WHERE kind='sources' AND id=$1", sourceID).Scan(&srcb); e != nil {
		return "", e
	}
	if e = json.Unmarshal(srcb, &src); e != nil {
		return "", e
	}
	if !src.Enabled {
		return "", errors.New("source paused")
	}
	id := ID()
	mb, _ := json.Marshal(meta)
	var key any
	if dedup != "" {
		key = sourceID + ":" + src.Epoch + ":" + dedup
	}
	if key != nil {
		var inserted string
		e = tx.QueryRow(ctx, "INSERT INTO source_dedup(key,event_id) VALUES($1,$2) ON CONFLICT DO NOTHING RETURNING event_id", key, id).Scan(&inserted)
		if errors.Is(e, pgx.ErrNoRows) {
			if e = tx.QueryRow(ctx, "SELECT event_id FROM source_dedup WHERE key=$1", key).Scan(&id); e != nil {
				return "", e
			}
			return id, tx.Commit(ctx)
		}
		if e != nil {
			return "", e
		}
	}
	var inserted string
	e = tx.QueryRow(ctx, "INSERT INTO events(id,source_id,dedup_key,raw,metadata) VALUES($1,$2,$3,$4,$5) ON CONFLICT(dedup_key) DO NOTHING RETURNING id", id, sourceID, key, raw, mb).Scan(&inserted)
	if errors.Is(e, pgx.ErrNoRows) {
		if e = tx.QueryRow(ctx, "SELECT id FROM events WHERE dedup_key=$1", key).Scan(&id); e != nil {
			return "", e
		}
		return id, tx.Commit(ctx)
	}
	if e != nil {
		return "", e
	}
	rows, e := tx.Query(ctx, "SELECT data FROM configs WHERE kind='routes'")
	if e != nil {
		return "", e
	}
	routes := []Route{}
	for rows.Next() {
		var b []byte
		var r Route
		if e = rows.Scan(&b); e != nil {
			rows.Close()
			return "", e
		}
		if e = json.Unmarshal(b, &r); e != nil {
			rows.Close()
			return "", e
		}
		if r.Enabled && r.SourceID == sourceID {
			routes = append(routes, r)
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return "", e
	}
	for _, r := range routes {
		if r.FilterPath != "" {
			v, err := Extract(raw, r.FilterPath)
			if err != nil {
				return "", fmt.Errorf("route filter: %w", err)
			}
			b, _ := json.Marshal(v)
			var expected any
			dec := json.NewDecoder(bytesReader(r.FilterValue))
			dec.UseNumber()
			if err = dec.Decode(&expected); err != nil {
				return "", err
			}
			want, _ := json.Marshal(expected)
			if string(b) != string(want) {
				continue
			}
		}
		for _, eid := range r.EndpointIDs {
			var b []byte
			var ep Endpoint
			if e = tx.QueryRow(ctx, "SELECT data FROM configs WHERE kind='endpoints' AND id=$1", eid).Scan(&b); e != nil {
				return "", e
			}
			if e = json.Unmarshal(b, &ep); e != nil {
				return "", e
			}
			did := ID()
			if _, e = tx.Exec(ctx, "INSERT INTO deliveries(id,event_id,source_id,route_id,endpoint_id,snapshot,idempotency_key) VALUES($1,$2,$3,$4,$5,$6,$1)", did, id, sourceID, r.ID, eid, b); e != nil {
				return "", e
			}
		}
	}
	return id, tx.Commit(ctx)
}
func bytesReader(b []byte) io.Reader { return &byteReader{b: b} }

type byteReader struct{ b []byte }

func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}
func (s *Store) Claim(ctx context.Context, eid string, n int) ([]Delivery, error) {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, `WITH chosen AS (SELECT id FROM deliveries WHERE endpoint_id=$1 AND status IN ('PENDING','RETRY_WAIT') AND next_at<=now() ORDER BY next_at,id LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE deliveries d SET status='SENDING',attempts=attempts+1,lease_token=$3,lease_until=now()+interval '30 seconds' FROM chosen c WHERE d.id=c.id RETURNING d.id,d.event_id,d.source_id,d.route_id,d.endpoint_id,d.snapshot,d.attempts,d.idempotency_key,d.lease_token`, eid, n, ID())
	if e != nil {
		return nil, e
	}
	out := []Delivery{}
	for rows.Next() {
		var d Delivery
		var b []byte
		if e = rows.Scan(&d.ID, &d.EventID, &d.SourceID, &d.RouteID, &d.EndpointID, &b, &d.Attempts, &d.IdempotencyKey, &d.Lease); e != nil {
			rows.Close()
			return nil, e
		}
		if e = json.Unmarshal(b, &d.Endpoint); e != nil {
			rows.Close()
			return nil, e
		}
		out = append(out, d)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	for i := range out {
		d := &out[i]
		var mb []byte
		if e = tx.QueryRow(ctx, "SELECT raw,metadata,created_at FROM events WHERE id=$1", d.EventID).Scan(&d.Event.Raw, &mb, &d.Event.ReceivedAt); e != nil {
			return nil, e
		}
		d.Event.ID = d.EventID
		d.Event.SourceID = d.SourceID
		if e = json.Unmarshal(mb, &d.Event.Metadata); e != nil {
			return nil, e
		}
		if _, e = tx.Exec(ctx, "INSERT INTO attempts(id,delivery_id,number) VALUES($1,$2,$3)", ID(), d.ID, d.Attempts); e != nil {
			return nil, e
		}
	}
	return out, tx.Commit(ctx)
}
func (s *Store) Complete(ctx context.Context, d Delivery, r Result) error {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	wait := time.Second * time.Duration(1<<min(d.Attempts-1, 8))
	wait = time.Duration(mathrand.Int64N(int64(wait)))
	if r.RetryAfter > wait {
		wait = r.RetryAfter
	}
	if wait > time.Hour {
		wait = time.Hour
	}
	status := r.Status
	if status == "RETRY_WAIT" && d.Attempts >= d.Endpoint.MaxAttempts {
		status = "DEAD"
	}
	tag, e := tx.Exec(ctx, `UPDATE deliveries SET status=CASE WHEN $3='RETRY_WAIT' AND deadline_at<=now() THEN 'DEAD' ELSE $3 END,error=$4,next_at=now()+$5*interval '1 millisecond',lease_token=NULL,lease_until=NULL,completed_at=CASE WHEN $3 IN ('SUCCEEDED','DEAD') THEN now() ELSE NULL END WHERE id=$1 AND lease_token=$2 AND status='SENDING'`, d.ID, d.Lease, status, r.Error, wait.Milliseconds())
	if e != nil {
		return e
	}
	if tag.RowsAffected() == 0 {
		return errors.New("lease lost; remote result may be unknown")
	}
	if _, e = tx.Exec(ctx, "UPDATE attempts SET status=$3,http_status=$4,error=$5,duration_ms=$6 WHERE delivery_id=$1 AND number=$2", d.ID, d.Attempts, r.Status, r.HTTPStatus, r.Error, r.Duration.Milliseconds()); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Recover(ctx context.Context) error {
	if _, e := s.DB.Exec(ctx, "UPDATE deliveries SET status='DEAD',error='maximum delivery age exceeded',completed_at=now() WHERE status IN ('PENDING','RETRY_WAIT') AND deadline_at<=now()"); e != nil {
		return e
	}
	_, e := s.DB.Exec(ctx, `UPDATE deliveries SET status=CASE WHEN attempts >= COALESCE((snapshot->>'max_attempts')::int,8) OR deadline_at<=now() THEN 'DEAD' ELSE 'RETRY_WAIT' END,error='previous send outcome unknown: lease expired',next_at=now(),lease_token=NULL,lease_until=NULL WHERE status='SENDING' AND lease_until<now()`)
	return e
}

func (s *Store) capacity(ctx context.Context) error {
	s.capacityMu.Lock()
	defer s.capacityMu.Unlock()
	if time.Since(s.capacityAt) < 5*time.Second {
		return s.capacityErr
	}
	var used int64
	e := s.DB.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&used)
	s.capacityAt = time.Now()
	s.capacityErr = e
	if e == nil && used >= s.MaxStorageBytes {
		s.capacityErr = errors.New("storage budget reached; admission paused")
	}
	return s.capacityErr
}

// Terminal records expire after seven days; unfinished records are never removed.
// Independent source dedup marks survive for thirty days.
func (s *Store) Cleanup(ctx context.Context) error {
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, `WITH old AS (SELECT id FROM deliveries WHERE status IN ('SUCCEEDED','DEAD','CANCELLED') AND created_at<now()-interval '7 days' ORDER BY created_at LIMIT 500), a AS (DELETE FROM attempts WHERE delivery_id IN (SELECT id FROM old)) DELETE FROM deliveries WHERE id IN (SELECT id FROM old)`)
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `DELETE FROM events WHERE id IN (SELECT e.id FROM events e WHERE created_at<now()-interval '7 days' AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.event_id=e.id) LIMIT 500)`); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `DELETE FROM source_dedup WHERE key IN (SELECT key FROM source_dedup WHERE expires_at<now() LIMIT 1000)`); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Deliveries(ctx context.Context, before string) ([]Delivery, error) {
	rows, e := s.DB.Query(ctx, `SELECT id,event_id,source_id,route_id,endpoint_id,status,attempts,error,created_at,next_at,idempotency_key FROM deliveries WHERE ($1='' OR (created_at,id)<(SELECT created_at,id FROM deliveries WHERE id=$1)) ORDER BY created_at DESC,id DESC LIMIT 100`, before)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Delivery{}
	for rows.Next() {
		var d Delivery
		if e = rows.Scan(&d.ID, &d.EventID, &d.SourceID, &d.RouteID, &d.EndpointID, &d.Status, &d.Attempts, &d.Error, &d.CreatedAt, &d.NextAt, &d.IdempotencyKey); e != nil {
			return nil, e
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
func (s *Store) Attempts(ctx context.Context, id string) ([]Attempt, error) {
	rows, e := s.DB.Query(ctx, "SELECT id,number,status,http_status,error,duration_ms,created_at FROM attempts WHERE delivery_id=$1 ORDER BY number", id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Attempt{}
	for rows.Next() {
		var a Attempt
		if e = rows.Scan(&a.ID, &a.Number, &a.Status, &a.HTTPStatus, &a.Error, &a.DurationMS, &a.CreatedAt); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (s *Store) Replay(ctx context.Context, id string) (string, error) {
	newID := ID()
	tag, e := s.DB.Exec(ctx, `INSERT INTO deliveries(id,event_id,source_id,route_id,endpoint_id,snapshot,idempotency_key) SELECT $2,event_id,source_id,route_id,endpoint_id,snapshot,idempotency_key FROM deliveries WHERE id=$1 AND status IN ('DEAD','SUCCEEDED','CANCELLED')`, id, newID)
	if e != nil {
		return "", e
	}
	if tag.RowsAffected() == 0 {
		return "", errors.New("only completed deliveries may be replayed")
	}
	_, e = s.DB.Exec(ctx, "INSERT INTO audit(action,object_id) VALUES('replay',$1)", id)
	return newID, e
}
func (s *Store) Stats(ctx context.Context) (map[string]int64, error) {
	r, e := s.DB.Query(ctx, "SELECT status,count(*) FROM deliveries GROUP BY status")
	if e != nil {
		return nil, e
	}
	defer r.Close()
	m := map[string]int64{}
	for r.Next() {
		var k string
		var n int64
		if e = r.Scan(&k, &n); e != nil {
			return nil, e
		}
		m[k] = n
	}
	return m, r.Err()
}
