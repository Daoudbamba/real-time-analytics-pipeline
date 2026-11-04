#!/usr/bin/env bash
# Creates .env from .env.example and fills every empty secret with a random value.
# Existing values are never replaced, and no secret is printed.
# Usage: bash scripts/init_env.sh
# shellcheck source=scripts/common.sh
source "$(dirname "$0")/common.sh"

random_token() {
    # URL-safe base64 of $1 random bytes.
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -base64 "$1"
    else
        head -c "$1" /dev/urandom | base64
    fi | tr '+/' '-_' | tr -d '\n'
}

if [[ ! -f .env ]]; then
    cp .env.example .env
    log "Created .env from .env.example."
fi

declare -A generators=(
    [POSTGRES_PASSWORD]="random_token 24 | tr -d '='"
    # Airflow requires exactly 32 bytes, padding included.
    [AIRFLOW_FERNET_KEY]="random_token 32"
    [AIRFLOW_JWT_SECRET]="random_token 48 | tr -d '='"
    [GRAFANA_ADMIN_PASSWORD]="random_token 24 | tr -d '='"
)

for name in "${REQUIRED_SECRETS[@]}"; do
    if [[ -n "$(env_value "$name")" ]]; then
        log "$name already set, kept."
        continue
    fi
    value="$(eval "${generators[$name]}")"
    if grep -q "^${name}=" .env; then
        # The generated characters (A-Z a-z 0-9 - _ =) are safe in a sed replacement.
        sed -i.bak "s|^${name}=.*|${name}=${value}|" .env && rm -f .env.bak
    else
        printf '%s=%s\n' "$name" "$value" >>.env
    fi
    log "$name generated."
done

log "Ready. Read the Grafana password with: grep GRAFANA_ADMIN_PASSWORD .env"
