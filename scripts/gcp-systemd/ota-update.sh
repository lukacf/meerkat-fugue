#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ota-update.sh [options]

Pulls the configured Fugue source checkout, builds Linux binaries on the host,
installs them atomically into the Fugue bin directory, restarts the service, and
verifies the result.

Options:
  --fugue-root PATH              Fugue host root. Default: $FUGUE_ROOT or /srv/fugue
  --repo PATH                    Source checkout. Default: $fugue_root/source/meerkat-fugue
  --remote NAME                  Git remote to fetch. Default: origin
  --branch NAME                  Git branch to deploy. Default: main
  --bin-dir PATH                 Install directory. Default: $fugue_root/bin
  --target-dir PATH              Cargo target dir. Default: $fugue_root/build-target/meerkat-fugue-prod-fast
  --service NAME                 systemd unit. Default: fugue-meerkat-fugue.service
  --service-manager KIND         auto, user, system, or none. Default: auto
  --env-file PATH                Optional env file to source before build/restart.
                                 Default: $fugue_root/secrets.env when present
  --port PORT                    Local health port. Default: 4001
  --health-url URL               Health URL. Default: http://127.0.0.1:$port/api/v1/state
  --health-timeout SECONDS       Health retry window after restart. Default: 30
  --skip-health                  Skip HTTP health check after restart.
  --replace-stray-processes      Kill old processes executing $bin_dir/meerkat-fugue
                                 before restarting if they are outside systemd.
  --dry-run                      Print planned actions without changing anything.
  -h, --help                     Show this help.

Build defaults intentionally favor fast repeat deploys on a trusted host:
  CARGO_PROFILE_RELEASE_OPT_LEVEL=0
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=256
  CARGO_PROFILE_RELEASE_DEBUG=false

Override those environment variables before invoking the script if you want a
fully optimized build.
USAGE
}

log() {
  printf '[fugue-ota] %s\n' "$*" >&2
}

die() {
  printf '[fugue-ota] error: %s\n' "$*" >&2
  exit 1
}

quote() {
  printf '%q' "$1"
}

run() {
  if [[ "$dry_run" == true ]]; then
    local rendered=
    local arg=
    for arg in "$@"; do
      rendered+=" $(quote "$arg")"
    done
    log "dry-run:${rendered}"
    return 0
  fi
  "$@"
}

fugue_root="${FUGUE_ROOT:-/srv/fugue}"
repo=
remote=origin
branch=main
bin_dir=
target_dir=
service=fugue-meerkat-fugue.service
service_manager=auto
env_file=
port=4001
health_url=
health_timeout=30
skip_health=false
replace_stray_processes=false
dry_run=false
runtime_stage_dir=
runtime_backup_dir=
runtime_binaries=(meerkat-fugue fugue-linear-mcp fugue-jira-mcp fugue-github-mcp fugue-host-mcp)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fugue-root)
      fugue_root="${2:?missing --fugue-root value}"
      shift 2
      ;;
    --repo)
      repo="${2:?missing --repo value}"
      shift 2
      ;;
    --remote)
      remote="${2:?missing --remote value}"
      shift 2
      ;;
    --branch)
      branch="${2:?missing --branch value}"
      shift 2
      ;;
    --bin-dir)
      bin_dir="${2:?missing --bin-dir value}"
      shift 2
      ;;
    --target-dir)
      target_dir="${2:?missing --target-dir value}"
      shift 2
      ;;
    --service)
      service="${2:?missing --service value}"
      shift 2
      ;;
    --service-manager)
      service_manager="${2:?missing --service-manager value}"
      shift 2
      ;;
    --env-file)
      env_file="${2:?missing --env-file value}"
      shift 2
      ;;
    --port)
      port="${2:?missing --port value}"
      shift 2
      ;;
    --health-url)
      health_url="${2:?missing --health-url value}"
      shift 2
      ;;
    --health-timeout)
      health_timeout="${2:?missing --health-timeout value}"
      shift 2
      ;;
    --skip-health)
      skip_health=true
      shift
      ;;
    --replace-stray-processes)
      replace_stray_processes=true
      shift
      ;;
    --dry-run)
      dry_run=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "$service_manager" in
  auto|user|system|none) ;;
  *) die "--service-manager must be auto, user, system, or none" ;;
esac
[[ "$health_timeout" =~ ^[0-9]+$ ]] || die "--health-timeout must be a non-negative integer"

repo="${repo:-$fugue_root/source/meerkat-fugue}"
bin_dir="${bin_dir:-$fugue_root/bin}"
target_dir="${target_dir:-$fugue_root/build-target/meerkat-fugue-prod-fast}"
health_url="${health_url:-http://127.0.0.1:$port/api/v1/state}"
if [[ -z "$env_file" && -f "$fugue_root/secrets.env" ]]; then
  env_file="$fugue_root/secrets.env"
fi

[[ -d "$repo/.git" ]] || die "repo is not a git checkout: $repo"
[[ -d "$bin_dir" ]] || die "bin dir does not exist: $bin_dir"
command -v git >/dev/null 2>&1 || die "git is required"
if [[ "$skip_health" != true ]]; then
  command -v curl >/dev/null 2>&1 || die "curl is required unless --skip-health is used"
fi

if [[ -n "$env_file" ]]; then
  [[ -f "$env_file" ]] || die "env file does not exist: $env_file"
  set -a
  # shellcheck disable=SC1090
  . "$env_file"
  set +a
fi

if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
fi

command -v cargo >/dev/null 2>&1 || die "cargo is required"

systemctl_args=()
systemctl_mutate=(systemctl)
resolved_manager="$service_manager"
if [[ "$service_manager" == user ]]; then
  systemctl_args=(--user)
elif [[ "$service_manager" == system ]]; then
  systemctl_args=()
elif [[ "$service_manager" == auto ]]; then
  user_unit=false
  system_unit=false
  if systemctl --user cat "$service" >/dev/null 2>&1; then
    user_unit=true
  fi
  if systemctl cat "$service" >/dev/null 2>&1; then
    system_unit=true
  fi
  if [[ "$user_unit" == true && "$system_unit" == true ]]; then
    die "found both user and system units named $service; pass --service-manager user or --service-manager system"
  elif [[ "$user_unit" == true ]]; then
    resolved_manager=user
    systemctl_args=(--user)
  elif [[ "$system_unit" == true ]]; then
    resolved_manager=system
    systemctl_args=()
  else
    die "could not find systemd unit $service with user or system manager"
  fi
fi
if [[ "$resolved_manager" == system && "$(id -u)" -ne 0 ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo is required to mutate system services as a non-root user"
  systemctl_mutate=(sudo -n systemctl)
else
  systemctl_mutate=(systemctl)
fi

lock_dir="$fugue_root/state/ota-update.lock"
if [[ "$dry_run" != true ]]; then
  mkdir -p "$(dirname "$lock_dir")"
  if ! mkdir "$lock_dir" 2>/dev/null; then
    die "another OTA update appears to be running: $lock_dir"
  fi
  cleanup_on_exit() {
    rm -rf "$lock_dir"
    if [[ -n "${runtime_stage_dir:-}" ]]; then
      rm -rf "$runtime_stage_dir"
    fi
    if [[ -n "${runtime_backup_dir:-}" ]]; then
      rm -rf "$runtime_backup_dir"
    fi
  }
  trap cleanup_on_exit EXIT
fi

log "repo: $repo"
log "target: $remote/$branch"
log "target dir: $target_dir"
log "bin dir: $bin_dir"
log "service manager: $resolved_manager"
log "service: $service"

cd "$repo"
previous_sha="$(git rev-parse --short HEAD)"
run git fetch --prune "$remote" "$branch"
if [[ "$dry_run" == true ]]; then
  target_sha="$remote/$branch"
else
  target_sha="$(git rev-parse --short FETCH_HEAD)"
fi
log "updating checkout: $previous_sha -> $target_sha"
run git reset --hard FETCH_HEAD

run mkdir -p "$target_dir"
export CARGO_TARGET_DIR="$target_dir"
export CARGO_PROFILE_RELEASE_OPT_LEVEL="${CARGO_PROFILE_RELEASE_OPT_LEVEL:-0}"
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS="${CARGO_PROFILE_RELEASE_CODEGEN_UNITS:-256}"
export CARGO_PROFILE_RELEASE_DEBUG="${CARGO_PROFILE_RELEASE_DEBUG:-false}"

run cargo build --release \
  --bin meerkat-fugue \
  --bin fugue-linear-mcp \
  --bin fugue-jira-mcp \
  --bin fugue-github-mcp \
  --bin fugue-host-mcp

release_dir="$target_dir/release"
if [[ "$dry_run" != true ]]; then
  for binary in "${runtime_binaries[@]}"; do
    [[ -x "$release_dir/$binary" ]] || die "built binary missing or not executable: $release_dir/$binary"
  done
fi

if [[ "$dry_run" != true ]]; then
  log "built hashes:"
  built_hash_paths=()
  for binary in "${runtime_binaries[@]}"; do
    built_hash_paths+=("$release_dir/$binary")
  done
  sha256sum "${built_hash_paths[@]}"
fi

meerkat_fugue_exe="$(readlink -f "$bin_dir/meerkat-fugue" 2>/dev/null || printf '%s' "$bin_dir/meerkat-fugue")"

matching_fugue_pids() {
  local pid=
  pgrep -x meerkat-fugue 2>/dev/null | while read -r pid; do
    [[ -n "$pid" ]] || continue
    local exe=
    exe="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
    if [[ "$exe" == "$meerkat_fugue_exe" || "$exe" == "$meerkat_fugue_exe (deleted)" ]]; then
      printf '%s\n' "$pid"
    fi
  done
}

terminate_fugue_pids_except() {
  local keep_pid="${1:-}"
  local pids=()
  local pid=
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    if [[ -n "$keep_pid" && "$pid" == "$keep_pid" ]]; then
      continue
    fi
    pids+=("$pid")
  done < <(matching_fugue_pids)
  if [[ "${#pids[@]}" -eq 0 ]]; then
    return 0
  fi
  log "terminating existing meerkat-fugue process(es): ${pids[*]}"
  run kill "${pids[@]}"
  sleep 2
  pids=()
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    if [[ -n "$keep_pid" && "$pid" == "$keep_pid" ]]; then
      continue
    fi
    pids+=("$pid")
  done < <(matching_fugue_pids)
  if [[ "${#pids[@]}" -gt 0 ]]; then
    log "force terminating existing meerkat-fugue process(es): ${pids[*]}"
    run kill -9 "${pids[@]}"
  fi
}

preflight_installed_runtime_set() {
  local binary=
  local live_path=
  for binary in "${runtime_binaries[@]}"; do
    live_path="$bin_dir/$binary"
    if [[ -L "$live_path" || ( -e "$live_path" && ! -f "$live_path" ) ]]; then
      die "installed runtime path is not a regular file: $live_path"
    fi
  done
}

stage_runtime_set() {
  runtime_stage_dir="$(mktemp -d "$fugue_root/state/ota-runtime-new.XXXXXX")"
  local binary=
  for binary in "${runtime_binaries[@]}"; do
    install -m 0755 "$release_dir/$binary" "$runtime_stage_dir/$binary"
  done
}

backup_runtime_set() {
  runtime_backup_dir="$(mktemp -d "$fugue_root/state/ota-runtime-backup.XXXXXX")"
  local binary=
  local live_path=
  for binary in "${runtime_binaries[@]}"; do
    live_path="$bin_dir/$binary"
    if [[ -e "$live_path" ]]; then
      cp -p "$live_path" "$runtime_backup_dir/$binary"
    fi
  done
}

rollback_runtime_set() {
  if [[ -z "${runtime_backup_dir:-}" || ! -d "$runtime_backup_dir" ]]; then
    log "rollback skipped: no runtime backup is available"
    return 0
  fi
  log "rolling back installed Fugue runtime binaries"
  local binary=
  local live_path=
  local restore_tmp=
  for binary in "${runtime_binaries[@]}"; do
    live_path="$bin_dir/$binary"
    restore_tmp="$bin_dir/.${binary}.rollback.$$"
    if [[ -e "$runtime_backup_dir/$binary" ]]; then
      cp -p "$runtime_backup_dir/$binary" "$restore_tmp"
      mv -f "$restore_tmp" "$live_path"
    else
      rm -f "$live_path" "$restore_tmp"
    fi
  done
}

stop_service_for_rollback() {
  if [[ "$resolved_manager" == none ]]; then
    return 0
  fi
  log "stopping failed $service before rollback"
  "${systemctl_mutate[@]}" "${systemctl_args[@]}" stop "$service" \
    || log "failed to stop $service before rollback; attempting process cleanup"
  terminate_fugue_pids_except "" \
    || log "failed to terminate running meerkat-fugue processes before rollback"
}

fail_after_replacement() {
  local message="$1"
  log "$message"
  stop_service_for_rollback
  rollback_runtime_set
  if [[ "$resolved_manager" != none ]]; then
    log "attempting to restart previous $service after rollback"
    "${systemctl_mutate[@]}" "${systemctl_args[@]}" restart "$service" || true
  fi
  die "$message"
}

replace_runtime_set() {
  local binary=
  for binary in "${runtime_binaries[@]}"; do
    if ! mv -f "$runtime_stage_dir/$binary" "$bin_dir/$binary"; then
      rollback_runtime_set
      die "failed to replace runtime binary: $bin_dir/$binary"
    fi
  done
}

if [[ "$dry_run" == true ]]; then
  for binary in "${runtime_binaries[@]}"; do
    run install -m 0755 "$release_dir/$binary" "$bin_dir/$binary.new.$$"
    run mv -f "$bin_dir/$binary.new.$$" "$bin_dir/$binary"
  done
else
  preflight_installed_runtime_set
  stage_runtime_set
  backup_runtime_set
  replace_runtime_set
fi

if [[ "$replace_stray_processes" == true ]]; then
  keep_pid=
  if [[ "$resolved_manager" != none && "$dry_run" != true ]]; then
    keep_pid="$(systemctl "${systemctl_args[@]}" show "$service" --property MainPID --value 2>/dev/null || true)"
  fi
  if ! terminate_fugue_pids_except "$keep_pid"; then
    fail_after_replacement "failed to terminate stray meerkat-fugue processes after runtime replacement"
  fi
fi

if [[ "$resolved_manager" != none ]]; then
  if ! run "${systemctl_mutate[@]}" "${systemctl_args[@]}" daemon-reload; then
    fail_after_replacement "systemd daemon-reload failed after runtime replacement"
  fi
  if ! run "${systemctl_mutate[@]}" "${systemctl_args[@]}" restart "$service"; then
    fail_after_replacement "systemd restart failed after runtime replacement"
  fi
  if [[ "$dry_run" != true ]]; then
    if ! systemctl "${systemctl_args[@]}" is-active "$service"; then
      fail_after_replacement "systemd service is not active after runtime replacement"
    fi
    if [[ "$replace_stray_processes" == true ]]; then
      main_pid="$(systemctl "${systemctl_args[@]}" show "$service" --property MainPID --value 2>/dev/null || true)"
      if ! terminate_fugue_pids_except "$main_pid"; then
        fail_after_replacement "failed to terminate stray meerkat-fugue processes after runtime replacement"
      fi
    fi
  fi
fi

if [[ "$skip_health" != true && "$dry_run" != true ]]; then
  curl_args=(-sS --max-time 10 --output /dev/null --write-out '%{http_code}')
  if [[ -n "${FUGUE_CONSOLE_USERNAME:-}" && -n "${FUGUE_CONSOLE_PASSWORD:-}" ]]; then
    curl_args=(-sS --max-time 10 --output /dev/null --write-out '%{http_code}' -u "$FUGUE_CONSOLE_USERNAME:$FUGUE_CONSOLE_PASSWORD")
  fi
  log "health: $health_url"
  start_seconds="$SECONDS"
  while true; do
    http_code="$(curl "${curl_args[@]}" "$health_url" 2>/dev/null || true)"
    case "$http_code" in
      2*|3*|401)
        log "health ok: HTTP $http_code"
        break
        ;;
    esac
    if (( SECONDS - start_seconds >= health_timeout )); then
      fail_after_replacement "health check failed after ${health_timeout}s: last HTTP status ${http_code:-none}"
    fi
    sleep 1
  done
fi

if [[ "$dry_run" != true ]]; then
  log "installed hashes:"
  installed_hash_paths=()
  for binary in "${runtime_binaries[@]}"; do
    installed_hash_paths+=("$bin_dir/$binary")
  done
  sha256sum "${installed_hash_paths[@]}"
fi

log "OTA update complete: $target_sha"
