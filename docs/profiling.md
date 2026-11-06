# 性能采样（pprof）

引擎把 Go 运行时的采样端点挂在管理接口上，默认关闭。开关是 `XW_PPROF`，
端点本身是标准库的 `/debug/pprof/`，没有自研实现。

## 开关

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `XW_PPROF` | 关 | 打开 `/debug/pprof/`；`1` / `true` / `yes` / `on` 视为开启 |
| `XW_PPROF_ALLOW_REMOTE` | 关 | 允许管理接口绑在非回环地址时也开启端点 |
| `XW_PPROF_BLOCK_RATE` | `0` | 阻塞采样阈值（纳秒），`0` 表示不采集；`10000` 即 10µs |
| `XW_PPROF_MUTEX_FRACTION` | `0` | 锁竞争采样比例，`1` 表示全采 |

端点挂在管理接口上，所以还要有 `XW_ADMIN_ADDR`（不配时等于 `ENGINE_PORT`，
默认 `127.0.0.1:8081`）。

## 端点

| 路径 | 回答什么 |
| --- | --- |
| `/debug/pprof/` | 索引页，列出各 profile 的当前样本数 |
| `/debug/pprof/profile?seconds=10` | CPU：时间花在哪些函数 |
| `/debug/pprof/heap` | 堆：谁在分配、谁常驻（`?debug=1` 是文本格式） |
| `/debug/pprof/goroutine?debug=1` | 当前有多少 goroutine、各自卡在哪一行 |
| `/debug/pprof/block` | 谁在等锁、等 channel（需先开 `XW_PPROF_BLOCK_RATE`） |
| `/debug/pprof/mutex` | 锁竞争有多严重（需先开 `XW_PPROF_MUTEX_FRACTION`） |
| `/debug/pprof/cmdline` · `symbol` · `trace` | 命令行、符号化、执行追踪 |

命名 profile 由索引处理器按路径后缀分发，因此没有逐个注册路由。

## 为什么默认关闭、只认回环

`cmdline` 会带出启动参数（可能含地址与账号），`heap` 的字符串内容里可能有消息
正文，`goroutine` 的栈能还原出内部结构。这些都不该在默认配置下从网络上可达。

管理接口没有独立鉴权，所以默认只在回环地址上开：`127.0.0.1`、`::1`、`localhost`
算回环，`:8081` 与 `0.0.0.0:8081` 表示监听所有网卡，按非回环处理。确实需要从
别的机器采集时（容器共用宿主网络，或经 SSH 转发到容器端口），显式设置
`XW_PPROF_ALLOW_REMOTE=1`，让这次暴露是一个看得见的决定。

启动日志会写明最终结果与原因，包括「配了 XW_PPROF 但管理接口关了」与「管理接口
不在回环地址」两种情况——抓不到 profile 的原因有一半是开关没生效，而端点没开时
返回 404，看起来像路径写错。

## 采样率

CPU、堆、goroutine 三类随时可抓，不需要预热。`block` 与 `mutex` 必须先把采样
打开，否则抓到的是空文件：空文件会被读成「没有阻塞」，这是这两类 profile 最
常见的误用。

两项都是进程级全局设置，在启动时一次性下发，不支持按请求切换——同一份 profile
里混着两种采样率，占比就失去意义了。

- `XW_PPROF_BLOCK_RATE` 是纳秒阈值，设 `1` 等于全采，开销最大；10µs 量级足够
  在压测里采到有意义的样本。
- `XW_PPROF_MUTEX_FRACTION` 是采样比例，`1` 表示每次竞争都记录；高频锁在 `1`
  下本身会成为可观的开销，长期开启要降到 100 之类的值。

## 采集流程

```bash
# 1. 带采样启动
XW_PPROF=1 XW_PPROF_BLOCK_RATE=10000 XW_PPROF_MUTEX_FRACTION=1 \
  ./bin/xw-engine

# 2. 在负载下抓 10 秒 CPU、一次堆与阻塞快照
curl -o cpu.pprof 'http://127.0.0.1:8081/debug/pprof/profile?seconds=10'
curl -o heap.pprof 'http://127.0.0.1:8081/debug/pprof/heap'
curl -o mutex.pprof 'http://127.0.0.1:8081/debug/pprof/mutex'

# 3. 分析（二进制路径用于符号化，须与运行的二进制一致）
go tool pprof -top ./bin/xw-engine cpu.pprof
go tool pprof -top -cum ./bin/xw-engine cpu.pprof
go tool pprof -http=:8082 ./bin/xw-engine cpu.pprof
```

## 实测结果

环境与 [benchmark.md](benchmark.md) 相同：Apple M4 Pro / Go 1.27.3 / Redis 8.10.1
本机回环 / 8 worker、`XW_PREFETCH=32` / 渠道全是 console mock。消息是社区点赞
事件，带步骤快照（聚合 → 站内信 → 推送），64 个帖子 ID 制造聚合竞争，队列
积压 30 万条时开始采样。

两组配置唯一的差别是收件箱存储：

| 收件箱 | 吞吐 | CPU profile 特征 |
| --- | --- | --- |
| JSONL 文件 | 约 100 条/秒 | `runtime.madvise` 占 40.7%（文件重写的分配与回收） |
| 内存实现 | 7,494 条/秒 | `syscall.rawsyscalln` 占 53.1%，Redis 客户端累计 30.8% |

差距 73 倍，问题不在 CPU 算力上。同一时刻抓的 goroutine 快照给出了直接原因：
8 个 worker 里有 6 个阻塞在 `JSONLInboxStore.Update` 的互斥锁上——该实现每次写入
都重写整份文件，文件越大单次更新越慢，8 个消费者于是排成一队。

换到内存收件箱后，热点回到预期位置：

| 位置 | 累计占比 |
| --- | --- |
| `handleDigest`（聚合步骤） | 34.0% |
| `redis.(*Client).Process` | 30.8% |
| 其中 `_getConn`（取连接） | 11.9% |

即：聚合步骤本身，以及它在 Redis 上的脚本往返，占了三分之一以上的采样，与
[benchmark.md](benchmark.md) 里「瓶颈是每条消息的脚本往返，不是 CPU」的结论一致。

锁竞争用 mutex profile 看更直接。同一轮负载下，`execution.(*JSONLStore).Append`
占锁等待的 **71%**（1.85s / 2.59s）：执行明细的全局写锁是继收件箱之后的第二个
串行点。这一条在压测报告里只能靠推断，profile 直接给出了比例。

`block` profile 在这份负载里 91% 的等待落在 `runtime.selectgo`，也就是 worker 在
等步骤结果的那个 select。它反映的是「在等下游」，不是锁竞争——判断锁要看 mutex
profile，两个 profile 回答的不是同一个问题。

## 改完收件箱之后的复核

收件箱改写改成「追加版本 + 按比例压缩」之后（格式与阈值见 [inbox.md](inbox.md)），
用同一份生产者脚本、同一台机器、同样的 8 worker 与 1000 张卡片重跑，只换实现：

| 收件箱实现 | 吞吐 | 文件 |
| --- | --- | --- |
| 改写整文件（旧） | 156 条/秒 | 332 KB |
| 追加版本 + 压缩（当前） | 13,104 条/秒 | 压缩后约 1 MB |

约 84 倍，与 goroutine 快照给出的方向一致：旧实现里 6/8 个 worker 卡在
`JSONLInboxStore.Update` 的互斥锁上，换掉整文件重写之后这处排队消失。

新实现的 CPU profile 里收件箱仍在，但成分变了：

| 位置 | 累计占比 |
| --- | --- |
| `JSONLInboxStore.Update` | 48.6% |
| 其中 `appendEntry` | 44.2% |
| 平铺热点 `syscall.rawsyscalln` | 62.3% |
| 压缩 `maybeCompact` | 2.2% |

也就是时间花在 `open`/`write`/`close` 本身，而不是读文件、解析 JSON 或分配内存。
下一步如果还要提速，方向是常驻文件句柄而不是继续改格式。

正确性一并校验：30 万条互动 → 1000 张卡片、每张 300 次、合计 300,000。

## 已知边界

- 上面的数字来自单机回环、生产者与消费者同机、渠道为 mock 的配置，不是线上容量
  结论；profile 本身也有采样偏差，比例看数量级即可。
- 端点没有鉴权，安全性完全依赖「只绑回环」这一条，加上 `XW_PPROF_ALLOW_REMOTE`
  的显式放行。前面挂反向代理时，代理层的鉴权不会自动覆盖到这个端口。
- 目前只有引擎接了采样端点，API 侧（Python）走的是各自的 profiling 手段，不在
  同一套开关里。
- 收件箱的整文件重写已按 profile 定位并改掉（见上一节）；执行明细的 JSONL 写入
  仍是一把全局锁 + 单次追加，多实例共享同一份文件时是下一个串行点。
