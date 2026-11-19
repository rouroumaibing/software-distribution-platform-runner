#!/usr/bin/env bash
# runner e2e-verify.sh — 真跑 agent 回连链的外部证据（hands and eyes）。
# runner 是 WS agent 执行器，自身不暴露 HTTP 端点；其"活着"的外部可查事实是 hub 的 runner 注册：
# Flow A 登录 hub -> GET /api/v1/ops/runners -> 校验目标 runner online + 心跳新鲜。
# 落请求/响应到 evidence/ 带时间戳 + sha256。零新依赖（curl + python3）。
# 用法: bash scripts/e2e-verify.sh
set -uo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STAMP="$(date -u +%FT%TZ | tr ':' '-')"
OUT="$REPO_DIR/evidence/$STAMP"
GW="${E2E_GATEWAY:-https://www.sdpworkflow.com:8443}"
RESOLVE="${E2E_RESOLVE:-www.sdpworkflow.com:8443:127.0.0.1}"
KC_ADMIN="${E2E_KC_ADMIN:-admin}"
KC_PASS="${E2E_KC_PASS:-sdp@12345}"
EXPECT_RUNNER="${E2E_RUNNER_NAME:-local-dev}"

CURL=(curl -sk --noproxy '*' --max-time 15 --resolve "$RESOLVE")
mkdir -p "$OUT/responses"
# session cookie 仅存于临时文件，退出即销毁；evidence/ 不留凭据。
JAR="$(mktemp "${TMPDIR:-/tmp}/sdp-e2e-cookie.XXXXXX")"
trap 'rm -f "$JAR"' EXIT
LOG="$OUT/e2e.log"
: > "$LOG"
say() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOG"; }
sha() { shasum -a 256 "$1" 2>/dev/null | awk '{print $1}'; }

# 1) Flow A 登录。
say "登录 hub (Flow A) -> $GW/api/v1/auth/login"
login_code=$("${CURL[@]}" -c "$JAR" -o "$OUT/login.json" -w '%{http_code}' \
  -X POST "$GW/api/v1/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"${KC_ADMIN}\",\"password\":\"${KC_PASS}\"}" 2>/dev/null)
if [ "$login_code" != "200" ]; then
  say "FAIL 登录失败 http=$login_code: $(head -c 200 "$OUT/login.json")"
  exit 1
fi
say "PASS 登录 http=200"

# 2) 查 runner 注册（回连事实）。
say "驱动 /api/v1/ops/runners (期望 runner=$EXPECT_RUNNER online + 心跳新鲜)"
body="$OUT/responses/runners.json"
code=$("${CURL[@]}" -b "$JAR" -o "$body" -w '%{http_code}' "$GW/api/v1/ops/runners" 2>/dev/null)
result=$(python3 - "$body" "$EXPECT_RUNNER" <<'PY'
import json, sys, datetime
d = json.load(open(sys.argv[1]))
expect = sys.argv[2]
items = (d.get('data') or {}).get('items') or []
hit = next((r for r in items if r.get('name') == expect), None)
if not hit:
    print("MISSING\t0\t0"); sys.exit(0)
hb = hit.get('lastHeartbeatAt', '')
fresh = 0
try:
    t = datetime.datetime.fromisoformat(hb.replace('Z', '+00:00'))
    fresh = 1 if (datetime.datetime.now(datetime.timezone.utc) - t).total_seconds() < 120 else 0
except Exception:
    pass
online = 1 if hit.get('online') and hit.get('status') == 'online' else 0
print(f"FOUND\t{online}\t{fresh}\t{hb}")
PY
)
IFS=$'\t' read -r found online fresh hb <<< "$result"
bytes=$(wc -c < "$body" | tr -d ' ')
ok=0
if [ "$code" = "200" ] && [ "$found" = "FOUND" ] && [ "$online" = "1" ] && [ "$fresh" = "1" ] && [ "$bytes" -gt 20 ]; then ok=1; fi
say "  $([ $ok -eq 1 ] && echo PASS || echo FAIL) http=$code found=$found online=$online fresh=$fresh hb=$hb bytes=$bytes sha=$(sha "$body" | cut -c1-12)"

# 3) manifest。
python3 - "$OUT/manifest.json" "$STAMP" "$GW" "$code" "$bytes" "$found" "$online" "$fresh" "$hb" "$ok" "$(sha "$body")" <<'PY'
import json, sys
(out, stamp, gw, code, bytes_, found, online, fresh, hb, ok, digest) = sys.argv[1:]
data = {
    "stamp": stamp, "gateway": gw,
    "chain": "flow-a-login -> session-cookie(temp only) -> ops/runners -> runner-registration(online+fresh heartbeat) -> evidence",
    "endpoint": "/api/v1/ops/runners", "expect_runner": "local-dev",
    "http": int(code), "bytes": int(bytes_), "found": found,
    "online": int(online), "heartbeat_fresh": int(fresh), "lastHeartbeatAt": hb,
    "ok": int(ok), "sha256": digest, "failures": 0 if ok == "1" else 1,
}
json.dump(data, open(out, 'w'), ensure_ascii=False, indent=2)
PY
cp "$LOG" "$OUT/e2e-final.log" 2>/dev/null || true
say "E2E $([ $ok -eq 1 ] && echo PASS || echo FAIL) failures=$([ $ok -eq 1 ] && echo 0 || echo 1) evidence=$OUT"
[ $ok -eq 1 ] || exit 1
exit 0
