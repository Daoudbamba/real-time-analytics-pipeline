# shellcheck shell=bash
# Helpers shared by the scripts in this folder. Sourced, never executed directly.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Git Bash on Windows rewrites arguments such as /opt/kafka into Windows paths. With the
# rewrite off, Docker needs the Windows form of the project path (C:/...) for bind mounts.
export MSYS_NO_PATHCONV=1
HOST_ROOT_DIR="$(pwd -W 2>/dev/null || pwd)"
export HOST_ROOT_DIR

REQUIRED_SECRETS=(POSTGRES_PASSWORD AIRFLOW_FERNET_KEY AIRFLOW_JWT_SECRET GRAFANA_ADMIN_PASSWORD)
# Below this, the full stack swaps and the Docker VM freezes (measured during step 6).
MIN_DOCKER_MEMORY_GIB=7

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { printf 'Error: %s\n' "$*" >&2; exit 1; }

# The scripts run from the project root, where Compose finds its file and .env.
compose() { docker compose "$@"; }

require_docker() {
    command -v docker >/dev/null 2>&1 || die "Docker is not installed."
    docker info >/dev/null 2>&1 || die "Docker is not running. Start Docker Desktop or the Docker service."
    docker compose version >/dev/null 2>&1 || die "Docker Compose v2 ('docker compose') is required."
}

# Reads one value from .env without exposing the others.
env_value() {
    sed -n "s/^$1=//p" .env | tail -n 1 | tr -d '"\r'
}

require_env() {
    [[ -f .env ]] || die ".env is missing: run 'bash scripts/init_env.sh' first."
    local missing=() name
    for name in "${REQUIRED_SECRETS[@]}"; do
        [[ -n "$(env_value "$name")" ]] || missing+=("$name")
    done
    ((${#missing[@]} == 0)) || die "empty in .env: ${missing[*]}. Run 'bash scripts/init_env.sh'."
}

warn_if_low_memory() {
    local bytes gib
    bytes="$(docker info --format '{{.MemTotal}}')"
    gib=$((bytes / 1024 / 1024 / 1024))
    if ((gib < MIN_DOCKER_MEMORY_GIB)); then
        log "Warning: Docker has ${gib} GiB of memory; the full stack needs about 8 GiB."
    fi
}

grafana_url() { echo "http://localhost:$(env_value GRAFANA_HOST_PORT | sed 's/^$/3000/')"; }
