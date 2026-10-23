#!/usr/bin/env bash
# xw-notify 端到端 demo：Python 触发 → Redis 队列 → Go engine 消费
#
# 前置依赖：
#   - Redis（默认 localhost:6379）
#   - Python 3.11+（api 依赖已安装，见 api/.venv）
#   - Go 1.22+（编译并运行 engine）
#
# 用法：
#   ./scripts/demo_e2e.sh
#
# 说明：脚本会依次启动 Redis（若未运行）、Python API（redis 模式）、
#       触发一条订单通知，再启动 Go engine 消费队列并打印 mock 发送日志。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_URL="${XW_REDIS_URL:-redis://localhost:6379/0}"
QUEUE_KEY="${XW_REDIS_QUEUE_KEY:-xw:queue:events}"
API_PORT="${API_PORT:-8000}"

log() { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }

# 1. 确保 Redis 可用
if ! redis-cli -u "$REDIS_URL" ping >/dev/null 2>&1; then
  log "Redis 未运行，尝试通过 docker 启动..."
  if command -v docker >/dev/null 2>&1; then
    docker run -d --name xw-notify-redis -p 6379:6379 redis:7-alpine >/dev/null
    sleep 1
  else
    log "未找到 docker，请先手动启动 Redis（$REDIS_URL）后重试。" >&2
    exit 1
  fi
fi
log "Redis 就绪：$REDIS_URL"

# 2. 启动 Python API（redis 总线模式）
log "启动 Python API（redis 模式，端口 $API_PORT）..."
if [ ! -x "$ROOT/api/.venv/bin/uvicorn" ]; then
  log "初始化 api 虚拟环境并安装依赖..."
  python3 -m venv "$ROOT/api/.venv"
  "$ROOT/api/.venv/bin/pip" install -q -r "$ROOT/api/requirements.txt"
fi
(
  cd "$ROOT/api"
  XW_BUS=redis XW_REDIS_URL="$REDIS_URL" \
    .venv/bin/uvicorn app.main:app --port "$API_PORT" --log-level warning
) &
API_PID=$!
trap 'kill "$API_PID" 2>/dev/null || true; kill "${ENGINE_PID:-}" 2>/dev/null || true' EXIT

# 等待 API 就绪
for _ in $(seq 1 30); do
  curl -sf "http://127.0.0.1:${API_PORT}/health" >/dev/null 2>&1 && break
  sleep 0.5
done
log "Python API 就绪：http://127.0.0.1:${API_PORT}"

# 3. 编译 Go engine（首次会下载依赖）
log "编译 Go engine（go build）..."
(
  cd "$ROOT/engine"
  mkdir -p bin
  go build -o "$ROOT/engine/bin/xw-engine" ./cmd/engine
)
ENGINE_BIN="$ROOT/engine/bin/xw-engine"
log "engine 编译完成：$ENGINE_BIN"

# 4. 触发一条订单通知（写入 Redis 队列）
log "触发订单通知（order_confirmed -> user_1）..."
TRIGGER_RESP="$(curl -s -X POST "http://127.0.0.1:${API_PORT}/v1/events/trigger" \
  -H 'Content-Type: application/json' \
  -d '{"name":"order_confirmed","to":{"subscriberId":"user_1","email":"hi@example.com","channels":["in_app","email"]},"payload":{"customer":"Alice","order_id":"A10293","amount":"199.00"}}')"
log "触发回执：$TRIGGER_RESP"

# 5. 启动 Go engine 消费队列（打印 mock 发送日志后 5 秒退出）
log "启动 Go engine 消费队列（观察下方 mock 发送日志）..."
(
  cd "$ROOT/engine"
  REDIS_HOST=localhost XW_REDIS_QUEUE_KEY="$QUEUE_KEY" XW_POLL_TIMEOUT=1 "$ENGINE_BIN"
) &
ENGINE_PID=$!

# 给 engine 数秒消费并打印日志
sleep 6
log "demo 结束。若上方出现 [mock] 发送通知 日志，说明端到端链路已打通。"
