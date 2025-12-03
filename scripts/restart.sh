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
        echo "[INFO] Stopping backend (pid ${SERVER_PID})..."
        kill "${SERVER_PID}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

ensure_database() {
    if [[ ! -d "${DB_PATH}" ]]; then
        echo "[INFO] Database directory ${DB_PATH} not found, creating..."
        mvn exec:java -Dexec.mainClass="com.minisql.backend.Launcher" \
            -Dexec.args="-create ${DB_PATH}"
        return
    fi

    local default_db_dir="${DB_PATH}/database"
    if [[ ! -d "${default_db_dir}" ]]; then
        echo "[INFO] Default database missing under ${DB_PATH}, creating..."
        mvn exec:java -Dexec.mainClass="com.minisql.backend.Launcher" \
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
    echo "[INFO] Backend did not become ready on port ${PORT}" >&2
    exit 1
}

echo "[INFO] Cleaning and compiling sources..."
mvn clean compile

ensure_database

echo "[INFO] Stopping any existing backend..."
pkill -f "com.minisql.backend.Launcher" >/dev/null 2>&1 || true

echo "[INFO] Starting backend on ${DB_PATH} (log: ${LOG_FILE})..."
mvn exec:java -Dexec.mainClass="com.minisql.backend.Launcher" \
    -Dexec.args="-open ${DB_PATH}" >"${LOG_FILE}" 2>&1 &
SERVER_PID=$!

wait_for_server
echo "[INFO] Backend is ready on port ${PORT}"
echo "[INFO] Launching client (Ctrl+D to exit, Ctrl+C to skip current line)..."

mvn exec:java -Dexec.mainClass="com.minisql.client.Launcher"
