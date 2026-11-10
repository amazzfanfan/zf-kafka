CREATE TABLE IF NOT EXISTS configs(kind text NOT NULL,id text NOT NULL,revision bigint NOT NULL DEFAULT 1,data jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(kind,id));
CREATE TABLE IF NOT EXISTS credentials(id text PRIMARY KEY,name text NOT NULL,ciphertext bytea NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS events(id text PRIMARY KEY,source_id text NOT NULL,dedup_key text UNIQUE,raw bytea NOT NULL,metadata jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS source_dedup(key text PRIMARY KEY,event_id text NOT NULL,expires_at timestamptz NOT NULL DEFAULT now()+interval '30 days');
CREATE INDEX IF NOT EXISTS source_dedup_expiry ON source_dedup(expires_at);
CREATE TABLE IF NOT EXISTS deliveries(id text PRIMARY KEY,event_id text NOT NULL REFERENCES events(id),source_id text NOT NULL,route_id text NOT NULL,endpoint_id text NOT NULL,snapshot jsonb NOT NULL,status text NOT NULL DEFAULT 'PENDING',attempts int NOT NULL DEFAULT 0,next_at timestamptz NOT NULL DEFAULT now(),deadline_at timestamptz NOT NULL DEFAULT now()+interval '24 hours',lease_token text,lease_until timestamptz,error text NOT NULL DEFAULT '',idempotency_key text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),completed_at timestamptz);
CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(endpoint_id,next_at,id) WHERE status IN ('PENDING','RETRY_WAIT');
CREATE INDEX IF NOT EXISTS deliveries_lease ON deliveries(lease_until) WHERE status='SENDING';
CREATE INDEX IF NOT EXISTS deliveries_created ON deliveries(created_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS attempts(id text PRIMARY KEY,delivery_id text NOT NULL REFERENCES deliveries(id),number int NOT NULL,status text NOT NULL DEFAULT 'UNKNOWN',http_status int NOT NULL DEFAULT 0,error text NOT NULL DEFAULT '',duration_ms bigint NOT NULL DEFAULT 0,created_at timestamptz NOT NULL DEFAULT now(),UNIQUE(delivery_id,number));
CREATE TABLE IF NOT EXISTS audit(id bigserial PRIMARY KEY,action text NOT NULL,object_id text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());

