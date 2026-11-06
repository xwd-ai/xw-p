# 执行明细的存储后端

执行明细按「触发 × 渠道」记账，回答的是「这个渠道最后发出去没有、失败原因是什么、
用了哪个实现」。引擎写、API 读（`GET /v1/executions`），因此两个进程必须看到同一份数据。

有两套后端，切换只改一个环境变量：

| 后端 | `XW_EXECUTION_STORE` | 定位 |
| --- | --- | --- |
| JSONL 文件（默认） | `jsonl` | 单机部署、本地开发。零依赖，一行一条，API 从文件尾部倒序读 |
| Postgres | `postgres` | 多实例部署。写入是一条 INSERT，查询侧的过滤、排序与游标分页下推到索引 |

单机时文件后端更快也更省事；一旦有多个引擎实例，它们会同时追加同一份文件，
只能靠「单次 `write` 在内核里原子」保证不把两条记录拼到一行，而且记录一多就无法
按渠道、状态、时间过滤。这时应该切到 Postgres。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `XW_EXECUTION_STORE` | `jsonl` | `jsonl` / `postgres`；写错的值回退 `jsonl` 并在启动日志里说明 |
| `XW_EXECUTION_FILE` | `api/data/executions.jsonl` | 文件后端的路径；写 `off` 表示完全关闭回写（两个后端都关） |
| `XW_DATABASE_URL` | 空 | Postgres 连接串，与 API 共用同一个变量。允许沿用 `postgresql+psycopg://` 写法，引擎连接前会换成 `postgres://` |
| `XW_PG_MAX_OPEN` | 8 | 最大打开连接数，超过 64 按 64 处理 |
| `XW_PG_MAX_IDLE` | 4 | 池里保留的空闲连接数，超过 `XW_PG_MAX_OPEN` 时被夹紧 |
| `XW_PG_CONN_LIFETIME` | 30m | 单条连接最长存活时间，用于换掉被中间设备静默断开的连接 |
| `XW_PG_CONN_IDLE_TIME` | 5m | 空闲连接回收时间 |
| `XW_PG_CONNECT_TIMEOUT` | 5s | 启动时连通性检查与预编译语句的超时 |
| `XW_PG_WRITE_TIMEOUT` | 5s | 单条明细的写入预算，超时只影响这一条记录 |

`XW_PG_MAX_OPEN` 不宜按「越大越快」调。执行明细是旁路写入，它抢走的连接会直接
压缩业务查询的可用配额；判断池子够不够看的是 `WaitCount` / `WaitDuration`——
引擎退出时会打一行连接池统计，这两个数只在所有连接被占满、调用方被迫排队时才增长。

## 表结构与列映射

引擎写的是 `scripts/migrations/004` 建立、`007` 补齐隔离维度的
`execution_details` 表，与 `api/app/execution_log.py` 的读取实现共用：

| 列 | 来源 | 说明 |
| --- | --- | --- |
| `id` | 数据库 `BIGSERIAL` | 由数据库生成，同时是查询翻页的次级排序键 |
| `trigger_id` | 消息的触发标识 | UUID 列；引擎在送库前校验形状，非法值不会产生一次必然失败的往返 |
| `channel` | 步骤结果的渠道 | `in_app` / `email` / `sms` / `push` / `chat` |
| `status` | 步骤结果 | `sent` / `failed` / `skipped` |
| `reason` | 失败或跳过原因 | 成功时为 `NULL` |
| `duration_ms` | 投递耗时 | 整数毫秒 |
| `created_at` | 数据库 `now()` | 由数据库生成 |
| `tenant` / `environment` | 消息的隔离维度 | 空值补 `default` / `development`，与表上的 `DEFAULT` 一致 |

JSONL 记录里还有 `step_id`、`provider`、`attempts`、`fallback` 四个排查字段，
表结构里没有对应列，切到 Postgres 后端时这几个字段**不落库**。这是当前的已知差异：
把它们补进表需要改迁移与 API 的读取映射，属于单独的一次改动。

## 失败语义

- **启动时连不上**：记一条错误日志，本次不回写执行明细，投递照常进行。
  数据库是旁路依赖，它不可用不该拦住消息本身。
- **启动时表不存在**：预编译语句失败，错误信息里直接指向建表迁移 `004`，
  避免对着 `42P01` 猜。
- **写入失败或超时**：记 `Warn`，消息仍然确认。让消息重投一次只会把通知再发一遍，
  记录没写上属于审计链路的问题。
- **连接串缺失但后端选了 postgres**：退回「不回写」并记错误。这是最容易配错的
  组合——切了后端却忘了填地址，表现出来只是明细为空。

## 事务与上下文

- 单条写入用一条预编译语句，构造时建一次、之后复用；`*sql.Stmt` 在连接失效时
  会自行重新预处理，不需要自己维护重试状态机。
- 批量写入（`AppendBatch`）在一个 `ReadCommitted` 事务里完成：要么整批落库，
  要么一条都不落。开事务之前先整批校验，一条不合法就一条都不写——审计链路里
  「写了一半」比「一条没写」更难查。
- `AppendContext` 接受调用方上下文：查询可以被取消或超时打断。有上下文的路径
  （延迟续跑的调度器）直接传下去；正常消费路径没有可传的上下文，由
  `XW_PG_WRITE_TIMEOUT` 兜住。

## 验证

```bash
# 单元测试：假驱动，覆盖参数顺序、事务边界、上下文取消、关闭语义、池参数
cd engine && go test ./internal/execution/ -run 'Postgres|Normalize' -v

# 真库用例：验证 SQL 在 Postgres 上的类型、默认值与 NULL 语义
XW_TEST_POSTGRES_DSN='postgres://user@localhost:5432/postgres?sslmode=disable' \
    go test ./internal/execution/ -run Integration -v
```

真库用例默认跳过（未设 `XW_TEST_POSTGRES_DSN` 时 skip），每次运行在独立 schema
里建表、跑完即 `DROP SCHEMA CASCADE`，不碰正式表。假驱动证明不了 SQL 在 Postgres
上语法正确，真库用例证明不了 Go 侧的池参数与事务边界，两者缺一不可。

## 已知缺口

- 只支持 URL 形式的连接串（`postgres://…`），关键字形式（`host=… user=…`）没有处理。
- 不负责建表：迁移仍由 `scripts/migrate_postgres.sh` 执行。
- `step_id` / `provider` / `attempts` / `fallback` 不落库（见上）。
- 真库用例不测连接被中途掐断的场景（`ConnMaxLifetime` 的实际回收行为）。
