# 健康检查

存活（liveness）与就绪（readiness）分开暴露，职责不同：

| 端点 | 用途 | 依赖 | 失败时 |
| --- | --- | --- | --- |
| `GET /health/live` | 进程是否还在跑 | 无 | 200（只有进程死了才失败） |
| `GET /health/ready` | 现在能不能接流量 | 总线 / 存储 / 消费线程 | 503 + 逐项原因 |
| `GET /health` | 人读的汇总 | 只读队列长度 | 恒 200 |
| `GET /healthz`（引擎） | 存活 | 无 | 200 |
| `GET /readyz`（引擎） | 就绪 | Redis PING | 503 + 逐项原因 |

## 为什么必须分开

把依赖塞进存活检查，一次 Redis 抖动会让编排器把整批实例重启掉：重启解决不了
依赖问题，却会丢掉正在处理的请求，把局部故障放大成全站故障。反过来，只做存活
检查又会让「服务正常但入不了队」的实例继续吃流量。两个问题需要两个信号。

`/health` 保留为汇总端点，给部署脚本与人用：它返回队列长度，但**不会**因为依赖
不可用而返回非 200，也不会因为队列读不到就 500（此时 `queued` 为 `null`，
日志里有一条 warning）。

## API 侧检查项

| 名称 | 检查内容 | 超时来源 |
| --- | --- | --- |
| `bus` | 内存后端恒通过；Redis 后端执行 `PING` | `XW_REDIS_TIMEOUT`（默认 5s，下限 2s） |
| `storage` | jsonl：数据目录存在且可写；postgres：独立短连接执行 `SELECT 1` | 连接与语句超时各 2s |
| `worker` | 仅在 `XW_API_WORKER` 开启时检查：内置消费线程是否存活 | 即时 |

响应形状：

```json
{
  "status": "degraded",
  "checks": [
    {"name": "bus", "status": "ok", "detail": "redis 可达", "duration_ms": 0.4},
    {"name": "storage", "status": "fail", "detail": "RuntimeError: ...", "duration_ms": 2.1},
    {"name": "worker", "status": "ok", "detail": "内置消费线程运行中", "duration_ms": 0.0}
  ]
}
```

全部通过时 `status` 为 `ok`、HTTP 200；任一失败为 `degraded`、HTTP 503。

几个刻意的取舍：

- **存储探测用独立短连接**，不复用业务连接池：业务池按生产负载配置，可能排队
  几十秒；探测要的是「两秒内能不能连上并查一句」的确定答案。连接与语句都带超时，
  数据库半死不活时探测自己会结束。
- **错误信息里抹掉凭据**：`scheme://user:pass@host` 会先被替换成 `scheme://***@host`。
  探测结果的可见范围通常比配置文件宽得多。
- **消费线程退出必须报 503**：线程死了队列会一直涨，而接口看起来完全正常，
  这是就绪检查最有价值的一类故障。
- **不检查渠道可用性**：某个渠道的上游超时属于业务健康，有 `/v1/providers` 的
  健康接口负责；混进就绪会让一次上游故障摘掉整个集群。

## 引擎侧检查项

`/readyz` 只检查消费链路的前提：Redis `PING`（超时 2s）。渠道探测结果不进就绪，
理由同上。

## 编排示例

```yaml
livenessProbe:
  httpGet: {path: /health/live, port: 8000}
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet: {path: /health/ready, port: 8000}
  periodSeconds: 5
  timeoutSeconds: 3   # 要大于单次探测的最坏耗时
  failureThreshold: 2
```

引擎的探针指向 `XW_ADMIN_ADDR` 所在端口（`/healthz` 与 `/readyz`）。该端口默认
关闭，需要探针时显式配置；对外暴露时同样应只开在内网。

## 验证

```bash
cd api
env -u HTTP_PROXY -u HTTPS_PROXY .venv/bin/python -m pytest tests/test_health.py -q

# 真实进程：内存总线 + jsonl 存储应直接 200
XW_API_WORKER=0 .venv/bin/python -m uvicorn app.main:app --port 8399
curl -s http://127.0.0.1:8399/health/ready | python -m json.tool
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8399/health/live

# Redis 后端指向一个不存在的实例：应 503 并点名 bus
XW_BUS=redis XW_REDIS_URL=redis://127.0.0.1:6399/0 XW_API_WORKER=0 \
  .venv/bin/python -m uvicorn app.main:app --port 8398
curl -s http://127.0.0.1:8398/health/ready | python -m json.tool

cd ../engine
go test ./internal/admin/ -run 'Ready|Health'
```
