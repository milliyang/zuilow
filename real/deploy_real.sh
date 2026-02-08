#!/bin/bash
# Real trading stack: start / stop / restart / logs / clean.
# DMS is external: ensure DMS is running and set DMS_BASE_URL in .env.
# No stime: zuilow and ppt use system time.
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -L)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -L)"
cd "$SCRIPT_DIR/docker"

_ensure_env() {
    if [ ! -f .env ]; then
        echo "Creating .env from env.example (edit .env to set DMS_BASE_URL if needed)"
        cp env.example .env
        _write_repo_root
    fi
}

_write_repo_root() {
    if grep -q '^REPO_ROOT=' .env 2>/dev/null; then
        sed -i.bak "s|^REPO_ROOT=.*|REPO_ROOT=$REPO_ROOT|" .env 2>/dev/null || true
        rm -f .env.bak
    else
        echo "REPO_ROOT=$REPO_ROOT" >> .env
    fi
}

_init() {
    if [ ! -f env.example ]; then
        echo "env.example not found in $SCRIPT_DIR/docker"
        exit 1
    fi
    cp env.example .env
    _write_repo_root
    echo "Created .env from env.example (REPO_ROOT=$REPO_ROOT). Edit real/docker/.env to set DMS_BASE_URL etc., then run $0 up."
}

_up() {
    _ensure_env
    _write_repo_root
    (cd "$SCRIPT_DIR" && mkdir -p run/zuilow run/ppt)
    echo "Starting real-zuilow, real-ppt..."
    REPO_ROOT="$REPO_ROOT" docker compose up -d --build --remove-orphans
    echo ""
    echo "=== Real trading stack is running ==="
    echo "  ZuiLow:   http://localhost:${ZUILOW_PORT:-11180}"
    echo "  PPT:      http://localhost:${PPT_PORT:-11182}"
    echo ""
    echo "DMS is external. Set DMS_BASE_URL in real/docker/.env if needed."
    echo "View logs: $0 logs [zuilow|ppt]"
    echo "Run dirs (log/DB): $SCRIPT_DIR/run/{zuilow,ppt}"
    echo ""
}

_down() {
    docker compose down "$@"
}

_stop() {
    docker compose stop "$@"
}

_restart() {
    _down
    _up
}

_logs() {
    docker compose logs -f "${@:-}"
}

_ps() {
    docker compose ps
}

_clean() {
    echo "Stopping and removing containers, networks, volumes..."
    docker compose down -v
    if [ "${1:-}" = "-y" ] || [ "${1:-}" = "--yes" ]; then
        docker image prune -f
        echo "Pruned unused images. Re-run $0 (or $0 up) to rebuild and start."
    else
        echo "Optionally prune unused images: $0 clean -y"
        echo "Re-run $0 (or $0 up) to start."
    fi
}

_usage() {
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  (none)   Show this usage. Use '$0 up' to start."
    echo "  init     Copy env.example to .env and set REPO_ROOT (run once or to reset .env)."
    echo "  up       Start real-zuilow, real-ppt (build if needed)."
    echo "  down     Stop and remove containers/networks. Add -v to remove volumes."
    echo "  stop     Stop containers (no remove)."
    echo "  restart  down + up."
    echo "  logs     Follow logs. Optional: service name(s), e.g. $0 logs zuilow ppt."
    echo "  ps       List containers (docker compose ps)."
    echo "  clean    down -v (remove volumes). Use 'clean -y' to also prune images."
    echo ""
    echo "Examples:"
    echo "  $0           # show usage"
    echo "  $0 init      # copy env.example -> .env"
    echo "  $0 up        # start"
    echo "  $0 down      # stop and remove"
    echo "  $0 down -v   # stop and remove volumes"
    echo "  $0 logs      # all services"
    echo "  $0 logs zuilow"
    echo "  $0 logs ppt"
    echo "  $0 clean     # down -v"
    echo "  $0 clean -y  # down -v + image prune"
}

case "${1:-}" in
    '')         _usage ;;
    init)       _init ;;
    up|start)   _up ;;
    down)       _down "${@:2}" ;;
    stop)       _stop "${@:2}" ;;
    restart)    _restart ;;
    logs)       _logs "${@:2}" ;;
    ps)         _ps ;;
    clean)      _clean "${2:-}" ;;
    -h|--help)  _usage ;;
    *)          echo "Unknown command: $1"; _usage; exit 1 ;;
esac
