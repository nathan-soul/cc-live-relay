#!/bin/sh
set -eu

PORT="${1:-8765}"
LISTEN_HOST="${2:-127.0.0.1}"

# Ports to open for the reverse proxy (Traefik/nginx). The relay port itself
# is NOT exposed to the outside world — the proxy reaches it on localhost.
FW_PORTS="${FW_PORTS:-80 443}"

RELAY_DIR=$(CDPATH= cd "$(dirname "$0")" && pwd)
VENV_DIR="$RELAY_DIR/.venv"

# --- Firewall helpers ---------------------------------------------------------
# Open the proxy ports so clients can reach the reverse proxy, and close them
# again when the relay exits or crashes. Pre-existing rules are left untouched.

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
        firewalld) $SUDO firewall-cmd --query-port="$1/tcp" >/dev/null 2>&1 ;;
        ufw)       $SUDO ufw status 2>/dev/null | grep -q "$1/tcp" ;;
        iptables)  $SUDO iptables -C INPUT -p tcp --dport "$1" -j ACCEPT >/dev/null 2>&1 ;;
        *)         return 1 ;;
    esac
}

open_port() {
    case "$FW_TOOL" in
        firewalld) $SUDO firewall-cmd --add-port="$1/tcp" >/dev/null 2>&1 ;;
        ufw)       $SUDO ufw allow "$1/tcp" >/dev/null 2>&1 ;;
        iptables)  $SUDO iptables -A INPUT -p tcp --dport "$1" -j ACCEPT >/dev/null 2>&1 ;;
    esac
}

close_port() {
    case "$FW_TOOL" in
        firewalld) $SUDO firewall-cmd --remove-port="$1/tcp" >/dev/null 2>&1 ;;
        ufw)       $SUDO ufw delete allow "$1/tcp" >/dev/null 2>&1 ;;
        iptables)  $SUDO iptables -D INPUT -p tcp --dport "$1" -j ACCEPT >/dev/null 2>&1 ;;
    esac
}

# Space-separated list of ports we opened; closed again on exit/crash.
OPENED=""
cleanup() {
    for p in $OPENED; do
        echo "Closing port ${p}/tcp ..."
        close_port "$p"
    done
}
trap cleanup EXIT

# --- Python / virtual environment ---------------------------------------------

PYTHON="$(command -v python3 || command -v python || true)"
if [ -z "$PYTHON" ]; then
    echo "Python not found. Install Python 3.8+."
    exit 1
fi

# Create the venv if missing, or recreate it when pip is unusable.
# Some distros (Debian/Ubuntu) build a venv without pip when python3-venv
# / ensurepip is not installed, so verify pip and not just bin/python.
if [ ! -x "$VENV_DIR/bin/python" ]; then
    echo "Creating virtual environment at .venv ..."
    if ! "$PYTHON" -m venv "$VENV_DIR"; then
        echo "Could not create a virtual environment."
        echo "Install the venv module (e.g. sudo apt install python3-venv) and retry."
        exit 1
    fi
fi

if ! "$VENV_DIR/bin/python" -m pip --version >/dev/null 2>&1; then
    echo "pip is missing in the virtual environment; recreating .venv ..."
    rm -rf "$VENV_DIR"
    if ! "$PYTHON" -m venv "$VENV_DIR" \
       || ! "$VENV_DIR/bin/python" -m pip --version >/dev/null 2>&1; then
        echo "The virtual environment has no pip (ensurepip is unavailable)."
        echo "Install python3-venv (e.g. sudo apt install python3-venv),"
        echo "delete the .venv directory, and retry."
        exit 1
    fi
fi

echo "Checking dependencies..."
if ! "$VENV_DIR/bin/python" -m pip install -q -r "$RELAY_DIR/requirements.txt"; then
    echo "pip install failed."
    exit 1
fi

# --- Firewall ports -------------------------------------------------------------

if [ -z "$FW_TOOL" ]; then
    echo "No firewall tool found (firewalld/ufw/iptables). Skipping port opening."
else
    for p in $FW_PORTS; do
        if port_is_open "$p"; then
            echo "Port ${p}/tcp is already open."
        elif open_port "$p"; then
            OPENED="$OPENED $p"
            echo "Opened port ${p}/tcp."
        else
            echo "WARNING: could not open port ${p}/tcp with $FW_TOOL."
            echo "         Check that sudo works for ${FW_TOOL} without a password prompt."
        fi
    done
fi

# --- Run the relay -------------------------------------------------------------

cd "$RELAY_DIR"
export HOST="$LISTEN_HOST"
export PORT="$PORT"

echo "Starting relay on $LISTEN_HOST:$PORT (reachable via the reverse proxy only) ..."
"$VENV_DIR/bin/python" server.py &
PY_PID=$!

trap 'kill "$PY_PID" 2>/dev/null' INT TERM

STATUS=0
wait "$PY_PID" || STATUS=$?
exit "$STATUS"
