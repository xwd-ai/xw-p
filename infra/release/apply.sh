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

# compose up 只重建配置有变化的容器，没重建的那个照样报健康，
# 跑的却是上一版镜像。就绪之后逐个核对实际运行的版本。
verify_release() {
    release=$1
    drift=
    for id in $(docker ps -q --filter label=com.docker.compose.project=xw-notify); do
        image=$(docker inspect -f '{{.Config.Image}}' "$id")
        case "$image" in "$prefix"-*) ;; *) continue ;; esac
        case "$image" in
            *:"$release") ;;
            *)
                service=$(docker inspect -f '{{index .Config.Labels "com.docker.compose.service"}}' "$id")
                drift="$drift $service=$image"
                ;;
        esac
    done
    if [ -n "$drift" ]; then
        echo "Release $release not applied:$drift" >&2
        return 1
    fi
}

rollback() {
    if [ -n "$previous" ] && [ "$previous" != "$tag" ]; then
        echo "Restoring $previous" >&2
        compose "$previous" up -d --wait --wait-timeout 180
    fi
}

compose "$tag" pull
if compose "$tag" up -d --wait --wait-timeout 180 && verify_release "$tag"; then
    printf '%s\n' "$tag" > "$root/current.tmp"
    mv "$root/current.tmp" "$root/current"
else
    rollback
    exit 1
fi
