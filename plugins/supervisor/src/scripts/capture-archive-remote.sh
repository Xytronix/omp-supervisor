#!/bin/bash
set -euo pipefail

forbidden=${SUPERVISOR_CAPTURE_FORBIDDEN:-}
valid() {
  local p="$1"
  [[ "$p" =~ ^/[A-Za-z0-9._/-]+$ ]] || return 1
  [[ "$p" != *..* ]] || return 1
  [[ "$p" != *"//"* ]] || return 1
  if [[ -n "$forbidden" ]]; then
    [[ "$p" == "$forbidden" || "$p" == "$forbidden"/* ]] && return 1
  fi
  return 0
}

op="${1:-}"
shift || true

case "$op" in
  mkdir)
    valid "$1"
    mkdir -p -- "$1"
    ;;
  sum)
    valid "$1"
    if [[ ! -f "$1" ]]; then
      printf 'MISSING\n'
      exit 0
    fi
    sha256sum -- "$1" | awk '{print $1}'
    stat -c '%s' -- "$1"
    ;;
  rename)
    valid "$1"
    valid "$2"
    [[ ! -e "$2" ]] || exit 4
    mv -T -- "$1" "$2"
    ;;
  rmfile)
    valid "$1"
    rm -f -- "$1"
    ;;
  listdates)
    valid "$1"
    [[ "$(basename -- "$1")" == "supervisor-captures" ]] || exit 3
    [[ -d "$1" ]] || exit 0
    ls -1 -- "$1"
    ;;
  rmdatedir)
    valid "$1"
    base="$(basename -- "$1")"
    parent="$(basename -- "$(dirname -- "$1")")"
    [[ "$parent" == "supervisor-captures" ]] || exit 3
    [[ "$base" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || exit 3
    rm -rf -- "$1"
    ;;
  *)
    exit 2
    ;;
esac
