#!/usr/bin/env bash
# verify-stamp.sh — 验证戳协议（harness-workflow §4.2）：给 commit 盖戳 + 查询覆盖率。
# 戳格式：commit message 末尾 trailer "Verified: <method>" / "Evidence: <path>"。
# none 是合法戳（可查询未验证提交）；白名单外的 method 视为无效戳（按未盖戳计）。
# 平台无关：只依赖 git，任何 agent / hook / CI 都能调用。
set -uo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VALID_METHODS="e2e unit property differential manual none"

usage() {
  cat <<EOF
用法: bash scripts/verify-stamp.sh <子命令> [参数]
  stamp   <method> [evidence]   输出可附到 commit message 的戳 trailer（method: ${VALID_METHODS}）
  check   [commit]              校验指定 commit（默认 HEAD）的戳是否有效，rc=0 有效 / rc=1 无效
  coverage [lookback]           统计回看窗口内带有效戳的 commit 覆盖率
EOF
}

valid_method() {
  local m="$1"
  for v in $VALID_METHODS; do [ "$m" = "$v" ] && return 0; done
  return 1
}

cmd_stamp() {
  local method="${1:-}" evidence="${2:-}"
  if ! valid_method "$method"; then
    echo "错误: 无效 method '${method}'（合法: ${VALID_METHODS}）" >&2
    return 1
  fi
  printf 'Verified: %s\n' "$method"
  if [ "$method" != "none" ] && [ -n "$evidence" ]; then
    printf 'Evidence: %s\n' "$evidence"
  fi
}

trailer_of() {
  local commit="$1"
  git -C "$REPO_DIR" log -1 --format='%B' "$commit" 2>/dev/null \
    | awk '/^Verified: /{v=$2} /^Evidence: /{e=$2} END{if(v!="")print v"\t"e}'
}

cmd_check() {
  local commit="${1:-HEAD}" trailer method
  trailer="$(trailer_of "$commit")"
  if [ -z "$trailer" ]; then
    echo "INVALID: 无 Verified trailer"
    return 1
  fi
  method="${trailer%%$'\t'*}"
  if ! valid_method "$method"; then
    echo "INVALID: method '$method' 不在白名单"
    return 1
  fi
  echo "VALID: $method"
  return 0
}

cmd_coverage() {
  local lookback="${1:-50}" total=0 valid=0 line method c
  for c in $(git -C "$REPO_DIR" log -n "$lookback" --format='%H' 2>/dev/null); do
    total=$((total + 1))
    line="$(trailer_of "$c")"
    if [ -n "$line" ]; then
      method="${line%%$'\t'*}"
      valid_method "$method" && valid=$((valid + 1))
    fi
  done
  if [ "$total" -eq 0 ]; then echo "coverage: 0/0 (n/a)"; return 0; fi
  echo "coverage: $valid/$total ($(awk -v a="$valid" -v b="$total" 'BEGIN{printf "%.1f%%",100*a/b}'))"
}

case "${1:-}" in
  stamp) shift; cmd_stamp "$@" ;;
  check) shift; cmd_check "$@" ;;
  coverage) shift; cmd_coverage "$@" ;;
  *) usage; exit 1 ;;
esac
