#!/usr/bin/env bash
# svc.sh — 本地开发服务启停（pid 文件 + 进程组双重校验）
# 用法: bash hack/svc.sh start | stop
# 设计:
#   - 启动前若检测到旧服务（pid 文件存在且进程存活）先按进程组清理，再启动新实例；
#   - 停止时同样依据 pid 文件（kill 进程组）+ 进程特征兜底清理；
#   - 进程以 setsid 自立会话，pid 文件记录 session leader，kill 进程组可连带
#     go run / 编译产物子进程，避免孤儿进程残留。
set -uo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(basename "$REPO_DIR")"
APP="${REPO#software-distribution-platform-}"            # console | hub | runner
RUN_DIR="$REPO_DIR/.run"
LOG="$RUN_DIR/dev.log"
PID="$RUN_DIR/dev.pid"
mkdir -p "$RUN_DIR"

proc_alive() { local p="$1"; [ -n "$p" ] && kill -0 "$p" 2>/dev/null; }

# 按 pid 文件清理（进程组）
kill_by_pidfile() {
  [ -f "$PID" ] || return 0
  local p; p="$(cat "$PID" 2>/dev/null || true)"
  if proc_alive "$p"; then
    echo "[svc:$APP] 终止旧服务进程组 pgid=$p"
    kill -TERM -"$p" 2>/dev/null
    local i
    for i in $(seq 1 10); do proc_alive "$p" || break; sleep 1; done
    if proc_alive "$p"; then
      echo "[svc:$APP] 超时未退出，强制终止 pgid=$p"
      kill -KILL -"$p" 2>/dev/null
    fi
  fi
  rm -f "$PID"
}

start_svc() {
  kill_by_pidfile                                    # 清理可能残留的旧服务（进程 + pid 双校验）
  pkill -f "software-distribution-platform-$APP" 2>/dev/null || true   # 兜底：按特征再扫一遍
  sleep 1
  local cmd
  case "$APP" in
    console) cmd="[ -d node_modules ] || npm install >/dev/null 2>&1; npm run dev" ;;
    hub)     cmd="go run ./cmd/hub" ;;
    runner)  cmd="go run ./cmd/runner" ;;
  esac
  echo "[svc:$APP] 启动本地开发服务 ..."
  setsid bash -c "cd '$REPO_DIR' && $cmd" >"$LOG" 2>&1 < /dev/null &
  local newpid=$!
  echo "$newpid" > "$PID"
  sleep 2
  if proc_alive "$newpid"; then
    echo "[svc:$APP] 已启动 pid=$newpid (日志: $LOG)"
  else
    echo "[svc:$APP] 警告: 进程已退出，请查看 $LOG"
  fi
}

stop_svc() {
  kill_by_pidfile
  # 兜底：pid 文件缺失或进程已脱离 pid 文件时，按进程特征清理
  if pgrep -f "software-distribution-platform-$APP" >/dev/null 2>&1; then
    echo "[svc:$APP] 兜底按进程特征清理 ..."
    pkill -TERM -f "software-distribution-platform-$APP" 2>/dev/null || true
    sleep 2
    pgrep -f "software-distribution-platform-$APP" >/dev/null 2>&1 && \
      pkill -KILL -f "software-distribution-platform-$APP" 2>/dev/null || true
  fi
  echo "[svc:$APP] 已停止本地开发服务。"
}

case "${1:-}" in
  start) start_svc ;;
  stop)  stop_svc ;;
  *) echo "usage: $0 <start|stop>"; exit 1 ;;
esac
