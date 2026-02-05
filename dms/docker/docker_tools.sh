#!/bin/bash
# DMS Docker Management Tools
# Convenience script for common Docker operations
# Run this script from the project root directory

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_DIR="$SCRIPT_DIR"
ENV_FILE="$DOCKER_DIR/.env"
ENV_EXAMPLE="$DOCKER_DIR/env.example"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Load compose file based on environment variable or .env file
get_compose_file() {
    local compose_mode="${COMPOSE_MODE:-}"
    
    # Try to load from .env file if exists
    if [ -f "$ENV_FILE" ]; then
        # Source .env file to get COMPOSE_MODE
        set -a
        source "$ENV_FILE" 2>/dev/null || true
        set +a
        compose_mode="${COMPOSE_MODE:-$compose_mode}"
    fi
    
    # Also check environment variable (highest priority)
    compose_mode="${COMPOSE_MODE:-$compose_mode}"
    
    # Default to internal (with InfluxDB)
    compose_mode="${compose_mode:-internal}"
    
    case "$compose_mode" in
        external|external-db)
            echo "$DOCKER_DIR/docker-compose.external-db.yml"
            ;;
        internal|default|"")
            echo "$DOCKER_DIR/docker-compose.yml"
            ;;
        *)
            print_warn "Unknown COMPOSE_MODE: $compose_mode, using default (internal)"
            echo "$DOCKER_DIR/docker-compose.yml"
            ;;
    esac
}

# Get compose file
COMPOSE_FILE=$(get_compose_file)

# Check if .env exists
check_env() {
    if [ ! -f "$ENV_FILE" ]; then
        print_warn ".env file not found. Creating from env.example..."
        if [ -f "$ENV_EXAMPLE" ]; then
            cp "$ENV_EXAMPLE" "$ENV_FILE"
            print_info ".env file created at $ENV_FILE"
            print_info "Please edit it with your configuration."
        else
            print_error "env.example not found at $ENV_EXAMPLE"
            exit 1
        fi
    fi
}

# Initialize: create .env and required directories
init() {
    print_info "Initializing DMS Docker environment..."
    check_env
    
    # Create required directories in project root
    cd "$PROJECT_ROOT"
    mkdir -p data run/logs run/db run/exports config
    
    print_info "Initialization complete!"
    print_info "Current compose file: $COMPOSE_FILE"
    print_info "Next steps:"
    echo "  1. Edit $ENV_FILE with your configuration"
    echo "     - Set COMPOSE_MODE=internal (with InfluxDB) or external (external InfluxDB)"
    echo "  2. Edit config/dms.yaml, config/task.yaml, config/sync.yaml"
    echo "  3. Run './docker/docker_tools.sh build' to build the image"
    echo "  4. Run './docker/docker_tools.sh up' to start services"
}

# Build Docker image
build() {
    print_info "Building DMS Docker image..."
    print_info "Using compose file: $COMPOSE_FILE"
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" build
    print_info "Build complete!"
}

# Start services (detached)
up() {
    check_env
    print_info "Starting DMS services..."
    print_info "Using compose file: $COMPOSE_FILE"
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d
    print_info "Services started!"
    print_info "DMS WebUI: http://localhost:${DMS_PORT:-11183}"
    if [[ "$COMPOSE_FILE" == *"external-db"* ]]; then
        print_info "Using external InfluxDB (configured in config/dms.yaml)"
    else
        print_info "InfluxDB UI: http://localhost:${INFLUXDB_PORT:-8086}"
    fi
}

# Start services (foreground, with logs)
upd() {
    check_env
    print_info "Starting DMS services (foreground)..."
    print_info "Using compose file: $COMPOSE_FILE"
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up
}

# Stop services
down() {
    print_info "Stopping DMS services..."
    print_info "Using compose file: $COMPOSE_FILE"
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" down
    print_info "Services stopped!"
}

# Restart services
restart() {
    print_info "Restarting DMS services..."
    print_info "Using compose file: $COMPOSE_FILE"
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" restart
    print_info "Services restarted!"
}

# View logs
logs() {
    cd "$PROJECT_ROOT"
    if [ -z "$1" ]; then
        docker compose -f "$COMPOSE_FILE" logs -f
    else
        docker compose -f "$COMPOSE_FILE" logs -f "$1"
    fi
}

# Execute shell in DMS container
shell() {
    print_info "Opening shell in DMS container..."
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" exec dms /bin/bash
}

# Execute Python in DMS container
python() {
    print_info "Running Python in DMS container..."
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" exec dms python "$@"
}

# Show container status
ps() {
    print_info "Container status:"
    cd "$PROJECT_ROOT"
    docker compose -f "$COMPOSE_FILE" ps
}

# Clean: remove containers, volumes, and images
clean() {
    print_warn "This will remove all containers, volumes, and images!"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "Cleaning up..."
        cd "$PROJECT_ROOT"
        docker compose -f "$COMPOSE_FILE" down -v --rmi all
        print_info "Cleanup complete!"
    else
        print_info "Cleanup cancelled."
    fi
}

# Show help
help() {
    echo "DMS Docker Management Tools"
    echo ""
    echo "Usage: ./docker/docker_tools.sh [command]"
    echo ""
    echo "Environment Variables:"
    echo "  COMPOSE_MODE    - Compose file mode: 'internal' (default, with InfluxDB) or 'external' (external InfluxDB)"
    echo "                    Can be set in .env file or as environment variable"
    echo ""
    echo "Commands:"
    echo "  init      - Initialize environment (create .env, directories)"
    echo "  build     - Build Docker image"
    echo "  up        - Start services (detached)"
    echo "  upd       - Start services (foreground with logs)"
    echo "  down      - Stop services"
    echo "  restart   - Restart services"
    echo "  logs      - View logs (optionally specify service: dms or influxdb)"
    echo "  shell     - Open shell in DMS container"
    echo "  python    - Run Python command in DMS container"
    echo "  ps        - Show container status"
    echo "  clean     - Remove all containers, volumes, and images"
    echo "  help      - Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./docker/docker_tools.sh init"
    echo "  ./docker/docker_tools.sh build"
    echo "  ./docker/docker_tools.sh up"
    echo "  ./docker/docker_tools.sh logs dms"
    echo "  ./docker/docker_tools.sh shell"
    echo ""
    echo "Using external InfluxDB:"
    echo "  COMPOSE_MODE=internal ./docker/docker_tools.sh up # 使用内部 InfluxDB(默认)"
    echo "  COMPOSE_MODE=external ./docker/docker_tools.sh up # 使用外部 InfluxDB"
    echo ""
    echo "Note: Run this script from the project root directory (sai/dms/)"
}

# Main command dispatcher
case "${1:-help}" in
    init)
        init
        ;;
    build)
        build
        ;;
    up)
        up
        ;;
    upd)
        upd
        ;;
    down)
        down
        ;;
    restart)
        restart
        ;;
    logs)
        logs "$2"
        ;;
    shell)
        shell
        ;;
    python)
        shift
        python "$@"
        ;;
    ps)
        ps
        ;;
    clean)
        clean
        ;;
    help|--help|-h)
        help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        help
        exit 1
        ;;
esac
