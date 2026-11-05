# 指标

API 暴露 `GET /metrics`，引擎运维端口暴露 `GET /metrics`，两者都是
Prometheus 文本格式（version 0.0.4），可直接被抓取端按实例采集。

## 为什么自己实现文本格式

需要导出的指标只有十来个（请求量、耗时、事件受理、幂等结果、限流拒绝、队列深度、
投递成功与失败）。抓取端只认文本，装一个客户端库带来的不只是那几行格式化代码，
还有「指标名怎么拼、`_total` 后缀怎么加、全局 registry 里还藏了哪些运行时指标」
这些需要额外解释的约定。这里实现 counter / gauge / histogram 三个类型，
转义、累积桶与排序都有测试盯住。

## API 侧指标

| 指标 | 类型 | 标签 | 含义 |
| --- | --- | --- | --- |
| `xw_http_requests_total` | counter | `method` `route` `status` | 请求量 |
| `xw_http_request_duration_seconds` | histogram | `method` `route` | 处理耗时（到响应开始为止） |
| `xw_http_in_flight_requests` | gauge | — | 正在处理的请求数 |
| `xw_events_triggered_total` | counter | `workflow` `environment` `result` | 互动事件受理数 |
| `xw_idempotency_requests_total` | counter | `result` | 幂等键的处理结果 |
| `xw_rate_limit_rejected_total` | counter | `dimension` | 被限流的请求数 |
| `xw_queue_depth` | gauge | `backend` | 待消费消息数（抓取时现算） |
| `xw_queue_enqueued_total` | counter | `backend` | 入队消息数 |
| `xw_build_info` | gauge | `version` `storage` `bus` `worker` | 进程身份，值恒为 1 |

口径说明：

- **`route` 是路由模板**（`/v1/workflows/{workflow_id}`），不是原始路径。按原始
  路径记，每来一个工作流标识就多一条序列，抓取端内存会跟着请求量涨。未匹配到
  路由的请求记成 `route="unknown"`。
- **`result` 三种取值**：`claimed`（首次占用幂等键并入队）、`replayed`（同键
  重放，返回既有回执）、`conflict`（键已被占用但回执读不出来，返回 409）。
- **`workflow` 只记真实模板名**；被拒绝的事件用 `workflow="unknown"`——请求体里
  的名称可以是任意字符串，按它建标签等于把基数交给调用方。
- **`/health*` 与 `/metrics` 不参与 HTTP 指标**：探针按秒打，会把真实流量淹没。
- 服务刚启动、还没有任何样本时，全部指标名照常输出（值为 0 或不带序列），
  免得抓取端把「还没流量」读成「指标消失了」。

## 引擎侧指标

| 指标 | 类型 | 标签 | 含义 |
| --- | --- | --- | --- |
| `xw_engine_uptime_seconds` | gauge | — | 统计窗口秒数（启动或上次重置至今） |
| `xw_delivery_total` | counter | `channel` `status` | 按渠道的投递次数 |
| `xw_provider_delivery_total` | counter | `provider` `status` | 按实现的投递次数 |
| `xw_delivery_retries_total` | counter | `channel` | 首次之外多打的尝试次数 |
| `xw_delivery_fallbacks_total` | counter | `channel` | 由备选实现完成的次数 |
| `xw_delivery_timeouts_total` | counter | `channel` | 超时放弃的次数 |
| `xw_delivery_duration_seconds` | histogram | `channel` | 投递耗时（含回退链上的失败尝试） |

- **不导出全渠道合计**：合计交给 `sum by (channel)`，多导一行同口径的数字只会在
  看板上制造一个必然对不齐的序列。
- 渠道与实现分成两组指标名，而不是共用一个名字配两套标签：同名不同标签集合的
  序列能写进同一个指标，但 `sum by` 会一半来自渠道、一半来自实现。
- 桶是非累积存储、累积导出：`le` 的语义是「小于等于该上界的样本数」。
- 与 `/admin/metrics` 的 JSON 是同一份快照，不存在两个口径；JSON 多了分位数与
  每桶增量，给运维脚本与后台页面用。

## 抓取与暴露面

```yaml
scrape_configs:
  - job_name: xw-api
    static_configs:
      - targets: ["api:8000"]
  - job_name: xw-engine
    static_configs:
      - targets: ["engine:8081"]   # XW_ADMIN_ADDR 指向的端口
```

两个 `/metrics` 都**不做鉴权**（抓取端通常没有凭据，指标也不属于任何租户）。
部署上要保证它们只在内网可达：绑定私网地址、用网关限制来源，或让编排器的
网络策略只放行抓取端。指标里不含请求体、地址与凭据，只有路由模板与配置里的
渠道名——但流量规模本身也是信息，不该暴露在公网。

## 验证

```bash
cd api
env -u HTTP_PROXY -u HTTPS_PROXY .venv/bin/python -m pytest tests/test_metrics.py -q

XW_API_WORKER=0 .venv/bin/python -m uvicorn app.main:app --port 8399
curl -s http://127.0.0.1:8399/metrics | head -20
curl -s -o /dev/null -H 'X-Request-Id: trace-1' http://127.0.0.1:8399/v1/workflows
curl -s http://127.0.0.1:8399/metrics | grep 'xw_http_requests_total{' | head -5

cd ../engine
go test ./internal/metrics/ ./internal/admin/ -run 'Prometheus|Metrics|Ready'
XW_ADMIN_ADDR=127.0.0.1:8081 go run ./cmd/engine
curl -s http://127.0.0.1:8081/metrics | head -10
```
