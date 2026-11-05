# 测试与覆盖率

## 命令

```bash
# API 与渠道实现（单文件调试时直接指定路径）
api/.venv/bin/python -m pytest api/tests -q

# 覆盖率：整套跑完才算数，低于门槛非零退出
api/.venv/bin/python scripts/coverage.py
api/.venv/bin/python scripts/coverage.py --floor 90 --xml coverage.xml

# Go：引擎与 SDK
cd engine && go test ./...
cd sdk/go && go test ./...

# 前端纯逻辑（Node 内置 runner，零依赖）
node --test 'dashboard/tests/*.test.js'

# OpenAPI 导出文件与实现是否一致
api/.venv/bin/python scripts/export_openapi.py --check
```

本机需要 `HTTP_PROXY` / `HTTPS_PROXY` 指向代理时，跑 Python 测试要先把它们去掉
（`env -u HTTP_PROXY -u HTTPS_PROXY ...`），否则测试里起的本地服务会走代理，
表现为「连不上」。`scripts/coverage.py` 已经代为清理。

## 覆盖率门槛

`scripts/coverage.py` 默认下限 **85%**，当前实测 **93.2%**（1216 个用例）。

下限的定位是防退化，不是考核指标：它挡住的是「顺手删掉一批测试」或「新增一大块
没人验的分支」。为了凑数字去测 getter 或者把断言写成恒真，只会让报告好看、
让重构更危险。因此：

- 门槛写在脚本里而不是 pytest 的 `addopts`：单文件调试时覆盖率天然很低，
  门槛会天天误报，人就会习惯加 `--no-cov`，门槛也就失效了。
- 门槛只对整套测试生效，CI 直接调用同一个脚本。
- 覆盖率下降时先看是什么代码进来了，而不是先补测试把数字压回去。

## 核心模块与测试对应

| 模块 | 关注点 | 测试文件 |
| --- | --- | --- |
| `app/workflow_store.py` | 名称唯一性按环境、行映射、Postgres SQL 形状 | `test_workflow_store.py`、`test_workflow_crud.py` |
| `app/storage.py` | jsonl 追加与统计、Postgres 参数与聚合、就绪探测 | `test_storage.py` |
| `app/idempotency.py` | 占用 / 回放 / 冲突三分支、多进程并发只入队一次 | `test_idempotency.py`、`test_idempotency_concurrency.py` |
| `app/ratelimit.py` | 令牌桶补桶、原子扣减、身份判定、后端故障放行 | `test_ratelimit.py` |
| `app/tenancy.py` | 租户与环境隔离、非法标识、跨租户 403 | `test_tenancy.py`、`test_environment_isolation.py` |
| `app/tokens.py` | HS256 编码、过期与时钟容差、`alg` 绕过、固定向量 | `test_admin_auth.py` |
| `app/metrics.py` | 文本格式转义、累积桶、抓取失败降级 | `test_metrics.py` |
| `app/health.py` | 存活与就绪分离、超时、错误文本脱敏 | `test_health.py` |
| `engine/internal/metrics` | 桶累积、标签转义、总和口径 | `prometheus_test.go` |
| `engine/internal/admin` | `/readyz` 语义、探测超时、`/metrics` 内容类型 | `health_test.go` |

## 已知缺口

诚实记录，避免「覆盖率 92% 所以哪都没问题」的误读：

- **Postgres 适配器用假引擎测**（`test_workflow_store.py`、`test_storage.py`）。
  验的是 SQL 形状、参数映射与行还原；连接池、事务与真实统计口径由
  `test_integration_phase*.py` 与部署环境覆盖。假引擎不能证明 SQL 在 PG 上
  语法正确。
- **内置消费线程的循环体只覆盖到单次处理**：`Worker._run` 的退出路径与
  异常重试依赖真实线程调度，测起来确定性差，风险由端到端用例兜。
- **`app/providers/*` 的外部 HTTP 实现**按契约测（mock 传输层），没有打真实
  供应商；真实凭据与网络行为不在仓库内验证。
- **前端只测纯逻辑**（收件箱组件、后台表单校验），DOM 渲染与交互靠人工冒烟。

## 写测试的约定

- 断言用「前后差值」而不是绝对累计值：指标、计数器这类进程级状态不会在用例间
  清零，绝对值写法单跑通过、整套失败，最难定位。
- 时间相关的用例注入时钟或使用毫秒级小超时，不 `sleep` 等真实窗口。
- 涉及外部依赖的测试必须先注入替身，再断言调用参数；连真库/真 Redis 的用例
  用 `tmp_path`、临时端口与独立进程，不与本地 demo 数据共用路径。
