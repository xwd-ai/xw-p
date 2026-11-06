#!/bin/sh
set -eu
umask 077

root="$HOME/.xw-notify"
if [ "${SSH_ORIGINAL_COMMAND:-}" = xw-release-check ]; then
    test -s "$root/runtime.env"
    test -x "$root/apply.sh"
    docker compose version --short
    exit
fi

set -f
set -- ${SSH_ORIGINAL_COMMAND:-}
if [ "$#" -ne 3 ] || [ "$1" != xw-release ]; then
    echo "Unsupported release command" >&2
    exit 1
fi
tag=$2
actor=$3
case "$tag" in ''|*[!0-9a-f]*) exit 1 ;; esac
case "$actor" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
[ "${#tag}" -eq 40 ]

IFS= read -r registry_token
test -n "$registry_token"
DOCKER_CONFIG=$(mktemp -d)
export DOCKER_CONFIG
trap 'rm -rf "$DOCKER_CONFIG"' EXIT
printf '%s\n' "$registry_token" | docker login ghcr.io --username "$actor" --password-stdin
unset registry_token

release_dir="$root/releases/$tag"
mkdir -p "$release_dir"
cat > "$release_dir/compose.yml.tmp"
test -s "$release_dir/compose.yml.tmp"
mv "$release_dir/compose.yml.tmp" "$release_dir/compose.yml"
"$root/apply.sh" ghcr.io/xwd-ai/xw-p "$tag"
