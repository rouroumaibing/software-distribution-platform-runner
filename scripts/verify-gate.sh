#!/usr/bin/env bash
# verify-gate.sh — 验证门（harness-workflow §4.5）：三档 + 拦截谓词 + 逃生门。
# 三档：off（不记录不拦）/ observe（只记录本会被拦的提交）/ enforce（拦，含被拦两次放行）。
# 拦截谓词：拦"当前会话内没有执行过约定验证动作的提交"——看会话内事实，不看 commit 戳。
# 会话内事实：状态文件标记本会话是否已跑过 VERIFY_CMD（off/observe/enforce 与验证戳互补）。
# 平台无关：状态文件 + git；可被 git pre-commit hook / agent 收尾动作 / CI 调用。
set -uo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STATE_DIR="$REPO_DIR/.run/verify-gate"
STATE="$STATE_DIR/session.json"
LOG="$STATE_DIR/intercept.log"
VERIFY_CMD="${VERIFY_CMD:-go test ./...}"
MODE_FILE="$STATE_DIR/mode"

mkdir -p "$STATE_DIR"

usage() {
  cat <<EOF
用法: bash scripts/verify-gate.sh <子命令> [参数]
  mode <off|observe|enforce>   设置当前档位（默认 observe）
  start-session                重置本会话状态（新 agent 任务开始时调用）
  mark-verified               标记本会话已执行过验证动作（验证命令成功后调用）
  check [commit]               按当前档位判定提交：observe 记录 / enforce 拦截
  status                       显示当前档位、会话状态、误拦日志计数
EOF
}

read_mode() { cat "$MODE_FILE" 2>/dev/null || echo observe; }
jget() { local k="$1"; awk -v k="\"$k\"" 'index($0,k){gsub(/[^0-9]/,"",$2);print $2}' "$STATE" 2>/dev/null || echo 0; }

cmd_mode() {
  case "${1:-}" in
    off|observe|enforce) printf '%s' "$1" > "$MODE_FILE"; echo "mode=$(read_mode)" ;;
    *) usage; return 1 ;;
  esac
}

cmd_start_session() {
  printf '{"verified":0,"intercepted":0,"started_at":"%s"}\n' "$(date -u +%FT%TZ)" > "$STATE"
  echo "session started"
}

cmd_mark_verified() {
  [ -f "$STATE" ] || cmd_start_session >/dev/null
  python3 - "$STATE" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p))
d['verified'] = 1
json.dump(d, open(p, 'w'))
PY
  echo "marked verified (cmd=$VERIFY_CMD)"
}

# 返回 0=通过 / 1=拦截；observe 恒 0（只记录）
cmd_check() {
  local commit="${1:-HEAD}" mode; mode="$(read_mode)"
  [ -f "$STATE" ] || cmd_start_session >/dev/null
  local verified intercepted method
  verified="$(python3 -c "import json;print(json.load(open('$STATE')).get('verified',0))" 2>/dev/null)"
  intercepted="$(python3 -c "import json;print(json.load(open('$STATE')).get('intercepted',0))" 2>/dev/null)"
  method="$(git -C "$REPO_DIR" log -1 --format='%B' "$commit" 2>/dev/null | awk '/^Verified: /{print $2;exit}')"
  method="${method:-none}"

  # 谓词：会话内没跑过验证动作 = 本会被拦
  if [ "$verified" = "1" ]; then
    echo "PASS: 会话内已执行 $VERIFY_CMD"
    return 0
  fi

  local reason="session-without-verify (Verified=${method})"
  case "$mode" in
    off) return 0 ;;
    observe)
      printf '%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$commit" "$reason" >> "$LOG"
      echo "OBSERVE: 本会被拦（${reason}），仅记录"
      return 0 ;;
    enforce)
      # 逃生门：同一会话被拦两次后，下一次放行（防 agent 死循环）
      if [ "$intercepted" -ge 2 ]; then
        printf '%s\t%s\tESCAPE after %s intercepts\n' "$(date -u +%FT%TZ)" "$commit" "$intercepted" >> "$LOG"
        echo "ESCAPE: 本会话已拦 $intercepted 次，放行"
        return 0
      fi
      python3 - "$STATE" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p))
d['intercepted'] = d.get('intercepted', 0) + 1
json.dump(d, open(p, 'w'))
PY
      printf '%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$commit" "$reason" >> "$LOG"
      echo "BLOCK: ${reason}（本会话第 ${intercepted}+1 次）" >&2
      return 1 ;;
  esac
}

cmd_status() {
  echo "mode=$(read_mode)"
  echo "state=$STATE"
  echo "verified=$(python3 -c "import json;print(json.load(open('$STATE')).get('verified',0))" 2>/dev/null || echo 0)"
  echo "intercepted=$(python3 -c "import json;print(json.load(open('$STATE')).get('intercepted',0))" 2>/dev/null || echo 0)"
  echo "intercept_log_lines=$(wc -l < "$LOG" 2>/dev/null | tr -d ' ')"
}

case "${1:-}" in
  mode) shift; cmd_mode "$@" ;;
  start-session) cmd_start_session ;;
  mark-verified) cmd_mark_verified ;;
  check) shift; cmd_check "$@" ;;
  status) cmd_status ;;
  *) usage; exit 1 ;;
esac
