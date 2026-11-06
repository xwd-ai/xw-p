#!/bin/sh
set -eu

prefix=${1:?Missing image prefix}
tag=${2:?Missing image tag}
valid_tag() {
    case "$1" in *[!0-9a-f]*|'') return 1 ;; esac
    [ "${#1}" -eq 40 ]
}
valid_tag "$tag" || { echo "Invalid image tag" >&2; exit 1; }

root="$HOME/.xw-notify"
exec 9>"$root/deploy.lock"
flock 9
previous=
if [ -f "$root/current" ]; then
    previous=$(cat "$root/current")
    valid_tag "$previous" || { echo "Invalid current release" >&2; exit 1; }
fi

compose() {
    release=$1
    shift
    XW_IMAGE_PREFIX="$prefix" XW_IMAGE_TAG="$release" docker compose \
        --project-name xw-notify --env-file "$root/runtime.env" \
        --file "$root/releases/$release/compose.yml" "$@"
}

compose "$tag" pull
if compose "$tag" up -d --wait --wait-timeout 180; then
    printf '%s\n' "$tag" > "$root/current.tmp"
    mv "$root/current.tmp" "$root/current"
else
    if [ -n "$previous" ] && [ "$previous" != "$tag" ]; then
        echo "Restoring $previous" >&2
        compose "$previous" up -d --wait --wait-timeout 180
    fi
    exit 1
fi
