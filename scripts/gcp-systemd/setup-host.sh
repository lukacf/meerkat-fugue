#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  setup-host.sh --fugue-root PATH [--service-user USER] [--bin-source DIR]

Prepares a trusted GCP/Linux host for Fugue queues. It creates the reusable
directory layout, a shared secrets.env placeholder, and a prepare-auth.sh hook.
If --bin-source is provided, it installs meerkat-fugue, fugue-linear-mcp,
fugue-github-mcp, and fugue-host-mcp into $fugue_root/bin.
USAGE
}

fugue_root=
service_user="${USER:-fugue}"
bin_source=

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fugue-root)
      fugue_root="${2:?missing --fugue-root value}"
      shift 2
      ;;
    --service-user)
      service_user="${2:?missing --service-user value}"
      shift 2
      ;;
    --bin-source)
      bin_source="${2:?missing --bin-source value}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$fugue_root" ]]; then
  echo "--fugue-root is required" >&2
  usage >&2
  exit 2
fi

owner_args=()
if [[ "$(id -u)" -eq 0 ]]; then
  owner_args=(-o "$service_user" -g "$service_user")
fi

install -d -m 0750 ${owner_args[@]+"${owner_args[@]}"} \
  "$fugue_root" \
  "$fugue_root/bin" \
  "$fugue_root/queues" \
  "$fugue_root/workspaces" \
  "$fugue_root/logs" \
  "$fugue_root/state" \
  "$fugue_root/context" \
  "$fugue_root/source"

if [[ ! -f "$fugue_root/secrets.env" ]]; then
  install -m 0600 ${owner_args[@]+"${owner_args[@]}"} /dev/null "$fugue_root/secrets.env"
  cat >"$fugue_root/secrets.env" <<'EOF'
# Fill these on the host. Do not commit this file.
LINEAR_API_KEY=
GITHUB_TOKEN=
GH_TOKEN=
OPENAI_API_KEY=
EOF
  if [[ "$(id -u)" -eq 0 ]]; then
    chown "$service_user:$service_user" "$fugue_root/secrets.env"
  fi
  chmod 0600 "$fugue_root/secrets.env"
fi

if [[ ! -f "$fugue_root/prepare-auth.sh" ]]; then
  cat >"$fugue_root/prepare-auth.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
# Host-local auth preparation hook. Keep secrets in secrets.env or provider
# auth stores, not in generated queue configs.
exit 0
EOF
  if [[ "$(id -u)" -eq 0 ]]; then
    chown "$service_user:$service_user" "$fugue_root/prepare-auth.sh"
  fi
  chmod 0750 "$fugue_root/prepare-auth.sh"
fi

if [[ -n "$bin_source" ]]; then
  install -m 0755 ${owner_args[@]+"${owner_args[@]}"} "$bin_source/meerkat-fugue" "$fugue_root/bin/meerkat-fugue"
  install -m 0755 ${owner_args[@]+"${owner_args[@]}"} "$bin_source/fugue-linear-mcp" "$fugue_root/bin/fugue-linear-mcp"
  install -m 0755 ${owner_args[@]+"${owner_args[@]}"} "$bin_source/fugue-github-mcp" "$fugue_root/bin/fugue-github-mcp"
  install -m 0755 ${owner_args[@]+"${owner_args[@]}"} "$bin_source/fugue-host-mcp" "$fugue_root/bin/fugue-host-mcp"
fi

cat <<EOF
Fugue host prepared:
  root: $fugue_root
  user: $service_user
  secrets: $fugue_root/secrets.env
  binaries: $fugue_root/bin
EOF
