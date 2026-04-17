#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
python_bin="${repo_root}/venv/bin/python"
checker_script="${repo_root}/check_bq_ch_sync.py"

ssh_target="${SYNC_TUNNEL_SSH_TARGET:-ansuman@ansuman-1}"
local_port="${SYNC_TUNNEL_LOCAL_PORT:-19000}"
remote_host="${SYNC_TUNNEL_REMOTE_HOST:-127.0.0.1}"
remote_port="${SYNC_TUNNEL_REMOTE_PORT:-9000}"

ch_user="${SYNC_CH_USER:-default}"
ch_password="${SYNC_CH_PASSWORD:-}"
ch_database="${SYNC_CH_DATABASE:-yral}"
ch_secure="${SYNC_CH_SECURE:-false}"
ch_verify="${SYNC_CH_VERIFY:-false}"

ssh_pid=""

cleanup() {
    if [[ -n "${ssh_pid}" ]] && kill -0 "${ssh_pid}" 2>/dev/null; then
        kill "${ssh_pid}" 2>/dev/null || true
        wait "${ssh_pid}" 2>/dev/null || true
    fi
}

wait_for_local_tunnel() {
    local attempts="${1}"
    local delay_seconds="${2}"
    local i

    for ((i = 1; i <= attempts; i += 1)); do
        if ! kill -0 "${ssh_pid}" 2>/dev/null; then
            echo "SSH tunnel process exited before the local forward became ready" >&2
            return 1
        fi

        if command -v ss >/dev/null 2>&1; then
            if ss -ltn "sport = :${local_port}" | tail -n +2 | grep -q .; then
                return 0
            fi
        elif command -v nc >/dev/null 2>&1; then
            if nc -z 127.0.0.1 "${local_port}" >/dev/null 2>&1; then
                return 0
            fi
        else
            if (echo >/dev/tcp/127.0.0.1/"${local_port}") >/dev/null 2>&1; then
                return 0
            fi
        fi

        sleep "${delay_seconds}"
    done

    echo "Timed out waiting for SSH tunnel on 127.0.0.1:${local_port}" >&2
    return 1
}

if [[ ! -x "${python_bin}" ]]; then
    echo "Missing Python venv at ${python_bin}" >&2
    exit 1
fi

if [[ ! -f "${checker_script}" ]]; then
    echo "Missing checker script at ${checker_script}" >&2
    exit 1
fi

if ! command -v ssh >/dev/null 2>&1; then
    echo "ssh is required to open the ClickHouse tunnel" >&2
    exit 1
fi

if command -v ss >/dev/null 2>&1; then
    if ss -ltn "sport = :${local_port}" | tail -n +2 | grep -q .; then
        echo "Local port ${local_port} is already in use. Set SYNC_TUNNEL_LOCAL_PORT to a free port." >&2
        exit 1
    fi
fi

trap cleanup EXIT

echo "Opening SSH tunnel to ${ssh_target} on localhost:${local_port} -> ${remote_host}:${remote_port}" >&2
ssh \
    -o ExitOnForwardFailure=yes \
    -o ConnectTimeout=10 \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o BatchMode=yes \
    -N \
    -L "${local_port}:${remote_host}:${remote_port}" \
    "${ssh_target}" &
ssh_pid=$!

if ! wait_for_local_tunnel 30 1; then
    exit 1
fi

export CH_HOST="127.0.0.1"
export CH_PORT="${local_port}"
export CH_USER="${ch_user}"
export CH_PASSWORD="${ch_password}"
export CH_DATABASE="${ch_database}"
export CH_SECURE="${ch_secure}"
export CH_VERIFY="${ch_verify}"

cd "${repo_root}"
"${python_bin}" "${checker_script}" "$@"
