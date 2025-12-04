#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DB_PATH="${1:-${MINISQL_DATA_DIR:-$ROOT_DIR/data}}"
PORT="${2:-9999}"
LOG_FILE="backend.log"

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

    # 如果目录存在但没有任何 .xid（没有数据库），则创建默认库
    if ! find "${DB_PATH}" -maxdepth 1 -name "*.xid" -print -quit >/dev/null; then
        echo "[INFO] No databases found under ${DB_PATH}, creating default..."
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

echo "[INFO] Stopping any existing backend..."
if [[ -f "backend.pid" ]]; then
    TARGET_PID=$(cat backend.pid)
    if ps -p "${TARGET_PID}" >/dev/null 2>&1; then
        kill -TERM "${TARGET_PID}" >/dev/null 2>&1 || true
        for _ in {1..10}; do
            if ! ps -p "${TARGET_PID}" >/dev/null 2>&1; then
                break
            fi
            sleep 0.5
        done
    fi
    rm -f backend.pid
fi

echo "[INFO] Cleaning and compiling sources..."
mvn clean compile

ensure_database

echo "[INFO] Starting backend on ${DB_PATH} (log: ${LOG_FILE})..."
mvn exec:java -Dexec.mainClass="com.minisql.backend.Launcher" \
    -Dexec.args="-open ${DB_PATH}" >"${LOG_FILE}" 2>&1 &
SERVER_PID=$!
echo "${SERVER_PID}" > backend.pid

wait_for_server
echo "[INFO] Backend is ready on port ${PORT}"
echo "[INFO] Launching client (Ctrl+D to exit, Ctrl+C to skip current line)..."

mvn exec:java -Dexec.mainClass="com.minisql.client.Launcher"
