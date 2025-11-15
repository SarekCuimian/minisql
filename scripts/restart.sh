#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-/tmp/minisql}"
PORT="${2:-9999}"
LOG_FILE="backend.log"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SERVER_PID=""
cleanup() {
    if [[ -n "${SERVER_PID}" ]] && ps -p "${SERVER_PID}" >/dev/null 2>&1; then
        echo "[mydb] Stopping backend (pid ${SERVER_PID})..."
        kill "${SERVER_PID}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

ensure_database() {
    if [[ ! -f "${DB_PATH}.db" ]]; then
        echo "[mydb] Database not found at ${DB_PATH}, creating..."
        mvn exec:java -Dexec.mainClass="top.guoziyang.mydb.backend.Launcher" \
            -Dexec.args="-create ${DB_PATH}"
    fi
}

wait_for_server() {
    for _ in {1..40}; do
        if (echo > /dev/tcp/127.0.0.1/"${PORT}") >/dev/null 2>&1; then
            return
        fi
        sleep 0.25
    done
    echo "[mydb] Backend did not become ready on port ${PORT}" >&2
    exit 1
}

echo "[mydb] Cleaning and compiling sources..."
mvn clean compile

ensure_database

echo "[mydb] Stopping any existing backend..."
pkill -f "top.guoziyang.mydb.backend.Launcher" >/dev/null 2>&1 || true

echo "[mydb] Starting backend on ${DB_PATH} (log: ${LOG_FILE})..."
mvn exec:java -Dexec.mainClass="top.guoziyang.mydb.backend.Launcher" \
    -Dexec.args="-open ${DB_PATH}" >"${LOG_FILE}" 2>&1 &
SERVER_PID=$!

wait_for_server
echo "[mydb] Backend is ready on port ${PORT}"
echo "[mydb] Launching client (Ctrl+D to exit, Ctrl+C to skip current line)..."

mvn exec:java -Dexec.mainClass="top.guoziyang.mydb.client.Launcher"
