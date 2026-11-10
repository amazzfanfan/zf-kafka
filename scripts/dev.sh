#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .local bin
if [[ ! -f .local/dev.env ]]; then
  python3 - <<'PY'
import secrets,pathlib,os
p=pathlib.Path('.local/dev.env')
p.write_text('export RELAY_ADMIN_TOKEN='+secrets.token_hex(24)+'\nexport RELAY_MASTER_KEY='+secrets.token_hex(32)+'\n')
os.chmod(p,0o600)
PY
fi
source .local/dev.env
if [[ -z "${DATABASE_URL:-}" ]]; then
  PG_BIN="${PG_BIN:-/opt/homebrew/opt/postgresql@17/bin}"
  if [[ ! -x "$PG_BIN/initdb" ]]; then
    echo 'Set DATABASE_URL for an existing PostgreSQL database, or PG_BIN for local PostgreSQL.' >&2
    exit 1
  fi
  if [[ ! -d .local/pg ]]; then "$PG_BIN/initdb" -D .local/pg -A trust -U relay --locale=C -E UTF8; fi
  if ! "$PG_BIN/pg_ctl" -D .local/pg status >/dev/null 2>&1; then
    "$PG_BIN/pg_ctl" -D .local/pg -l .local/postgres.log -o '-p 55432 -h 127.0.0.1' start
  fi
  "$PG_BIN/createdb" -h 127.0.0.1 -p 55432 -U relay relay >/dev/null 2>&1 || true
  export DATABASE_URL='postgres://relay@127.0.0.1:55432/relay?sslmode=disable'
fi
GO_BIN="${GO_BIN:-go}"
if ! command -v "$GO_BIN" >/dev/null 2>&1; then GO_BIN="$PWD/.tools/go/bin/go"; fi
if [[ ! -x "$GO_BIN" ]] && ! command -v "$GO_BIN" >/dev/null 2>&1; then echo 'Install Go >=1.26 or set GO_BIN.' >&2;exit 1;fi
(cd web && npm ci && npm run build)
"$GO_BIN" build -trimpath -o bin/relay ./cmd/relay
export APP_PREFIX="${APP_PREFIX:-relay}"
export RELAY_ALLOWED_CIDRS="${RELAY_ALLOWED_CIDRS:-127.0.0.0/8,::1/128}"
echo "Console: http://${RELAY_LISTEN:-127.0.0.1:8080}/$APP_PREFIX/"
echo 'Local admin token and encryption key: .local/dev.env (keep this file private).'
exec bin/relay
