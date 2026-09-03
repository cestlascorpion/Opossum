#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MYSQL_DIR=$(mktemp -d "${TMPDIR:-/tmp}/opossum-mysql.XXXXXX")
ETCD_DIR=$(mktemp -d "${TMPDIR:-/tmp}/opossum-etcd.XXXXXX")
MYSQL_SOCKET="$MYSQL_DIR/mysql.sock"
MYSQL_PORT=33306
ETCD_CLIENT_PORT=32379
ETCD_PEER_PORT=32380

cleanup() {
    if [[ -n "${MYSQL_PID:-}" ]] && kill -0 "$MYSQL_PID" 2>/dev/null; then
        mysqladmin --no-defaults --socket="$MYSQL_SOCKET" -u root shutdown >/dev/null 2>&1 || true
        wait "$MYSQL_PID" 2>/dev/null || true
    fi
    if [[ -n "${ETCD_PID:-}" ]] && kill -0 "$ETCD_PID" 2>/dev/null; then
        kill -TERM "$ETCD_PID" 2>/dev/null || true
        wait "$ETCD_PID" 2>/dev/null || true
    fi
    rm -rf "$MYSQL_DIR" "$ETCD_DIR"
}
trap cleanup EXIT INT TERM

command -v mysqld >/dev/null
command -v mysqladmin >/dev/null
command -v etcd >/dev/null
command -v etcdctl >/dev/null

mysqld --no-defaults --initialize-insecure --datadir="$MYSQL_DIR" >/dev/null 2>&1
mysqld --no-defaults \
    --datadir="$MYSQL_DIR" \
    --port="$MYSQL_PORT" \
    --bind-address=127.0.0.1 \
    --socket="$MYSQL_SOCKET" \
    --pid-file="$MYSQL_DIR/mysql.pid" \
    --log-error="$MYSQL_DIR/mysql.log" &
MYSQL_PID=$!

etcd \
    --name opossum-test \
    --data-dir="$ETCD_DIR" \
    --listen-client-urls="http://127.0.0.1:$ETCD_CLIENT_PORT" \
    --advertise-client-urls="http://127.0.0.1:$ETCD_CLIENT_PORT" \
    --listen-peer-urls="http://127.0.0.1:$ETCD_PEER_PORT" \
    --initial-advertise-peer-urls="http://127.0.0.1:$ETCD_PEER_PORT" \
    --initial-cluster="opossum-test=http://127.0.0.1:$ETCD_PEER_PORT" \
    >"$ETCD_DIR/etcd.log" 2>&1 &
ETCD_PID=$!

for _ in {1..60}; do
    if mysqladmin --no-defaults --socket="$MYSQL_SOCKET" -u root ping >/dev/null 2>&1; then
        break
    fi
    sleep 0.25
done
mysqladmin --no-defaults --socket="$MYSQL_SOCKET" -u root ping >/dev/null
kill -0 "$MYSQL_PID"

for _ in {1..60}; do
    if etcdctl --endpoints="http://127.0.0.1:$ETCD_CLIENT_PORT" endpoint health >/dev/null 2>&1; then
        break
    fi
    sleep 0.25
done
etcdctl --endpoints="http://127.0.0.1:$ETCD_CLIENT_PORT" endpoint health >/dev/null
kill -0 "$ETCD_PID"

cd "$ROOT_DIR"
OPOSSUM_TEST_MYSQL_ADDR="127.0.0.1:$MYSQL_PORT" \
OPOSSUM_TEST_MYSQL_USER=root \
OPOSSUM_TEST_MYSQL_PASSWORD= \
OPOSSUM_TEST_ETCD_ENDPOINT="127.0.0.1:$ETCD_CLIENT_PORT" \
go test -race -count=1 -timeout=90s -tags=integration ./test

go test ./...
go vet ./...
