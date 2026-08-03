#!/usr/bin/env bash
set -euo pipefail

PORT="${1:-8765}"
LISTEN_HOST="${2:-0.0.0.0}"

RELAY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Firewall helpers ---------------------------------------------------------
# Open TCP ${PORT} so players can reach the relay, and close it again when the
# relay exits or crashes. A pre-existing rule is left untouched.

# Firewall commands need root, so use sudo unless already running as root.
SUDO=""
if [ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
fi

FW_TOOL=""
if command -v firewall-cmd >/dev/null 2>&1; then
    FW_TOOL="firewalld"
elif command -v ufw >/dev/null 2>&1; then
    FW_TOOL="ufw"
elif command -v iptables >/dev/null 2>&1; then
    FW_TOOL="iptables"
fi

# Firewall tools often live in /sbin or /usr/sbin, which a non-root PATH may miss.
if [ -z "$FW_TOOL" ]; then
    for d in /usr/sbin /sbin; do
        if [ -x "$d/firewall-cmd" ]; then
            FW_TOOL="firewalld"
        elif [ -x "$d/ufw" ]; then
            FW_TOOL="ufw"
        elif [ -x "$d/iptables" ]; then
            FW_TOOL="iptables"
        fi
        [ -n "$FW_TOOL" ] && break
    done
fi

port_is_open() {
    case "$FW_TOOL" in
        firewalld) $SUDO firewall-cmd --query-port="${PORT}/tcp" >/dev/null 2>&1 ;;
        ufw)       $SUDO ufw status 2>/dev/null | grep -q "${PORT}/tcp" ;;
        iptables)  $SUDO iptables -C INPUT -p tcp --dport "${PORT}" -j ACCEPT >/dev/null 2>&1 ;;
        *)         return 1 ;;
    esac
}

open_port() {
    case "$FW_TOOL" in
        firewalld) $SUDO firewall-cmd --add-port="${PORT}/tcp" >/dev/null 2>&1 ;;
        ufw)       $SUDO ufw allow "${PORT}/tcp" >/dev/null 2>&1 ;;
        iptables)  $SUDO iptables -A INPUT -p tcp --dport "${PORT}" -j ACCEPT >/dev/null 2>&1 ;;
    esac
}

close_port() {
    case "$FW_TOOL" in
        firewalld) $SUDO firewall-cmd --remove-port="${PORT}/tcp" >/dev/null 2>&1 ;;
        ufw)       $SUDO ufw delete allow "${PORT}/tcp" >/dev/null 2>&1 ;;
        iptables)  $SUDO iptables -D INPUT -p tcp --dport "${PORT}" -j ACCEPT >/dev/null 2>&1 ;;
    esac
}

OPENED=0
cleanup() {
    if [ "$OPENED" = "1" ]; then
        echo "Closing port ${PORT}/tcp ..."
        close_port
    fi
}
trap cleanup EXIT

PYTHON="$(command -v python3 || command -v python || true)"
if [ -z "$PYTHON" ]; then
    echo "Python not found. Install Python 3.8+."
    exit 1
fi

echo "Checking dependencies..."
if ! "$PYTHON" -m pip install -q -r "$RELAY_DIR/requirements.txt"; then
    echo "pip install failed."
    exit 1
fi

if [ -z "$FW_TOOL" ]; then
    echo "No firewall tool found (firewalld/ufw/iptables). Skipping port opening."
elif port_is_open; then
    echo "Port ${PORT}/tcp is already open."
else
    echo "Opening port ${PORT}/tcp for incoming players ..."
    if open_port; then
        OPENED=1
    else
        echo "WARNING: could not open port ${PORT}/tcp with $FW_TOOL."
        echo "         Check that sudo works for ${FW_TOOL} without a password prompt."
    fi
fi

cd "$RELAY_DIR"
export HOST="$LISTEN_HOST"
export PORT="$PORT"

echo "Starting relay on $LISTEN_HOST:$PORT ..."
"$PYTHON" server.py &
PY_PID=$!

trap 'kill "$PY_PID" 2>/dev/null' INT TERM

STATUS=0
wait "$PY_PID" || STATUS=$?
exit "$STATUS"
