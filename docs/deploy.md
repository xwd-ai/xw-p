# 容器化部署

API 与引擎各有一个镜像定义（`api/Dockerfile`、`engine/Dockerfile`），
`infra/docker-compose.yml` 把它们与 Redis / Postgres / RabbitMQ 编排在一起，
一条命令拉起完整链路。

## 一键启动

```bash
cd infra
docker compose up -d --build api engine redis
```

服务就绪后按 README「快速开始」里的 curl 触发一条通知，收件箱接口应能读回
引擎写入的站内信。`docker compose ps` 里两个服务都应为 `healthy`：

- `api` 的健康检查打 `/health/ready`（总线 / 存储 / 消费线程逐项检查）；
- `engine` 的健康检查打 `/healthz`（存活，不碰依赖）。

只起依赖、代码在本机跑的开发方式不变：`docker compose up -d postgres redis
rabbitmq` 后照 README 起 uvicorn 与引擎。

## PostgreSQL 建表与升级

使用 PostgreSQL 持久化时，先设置 `XW_DATABASE_URL`，从仓库根目录执行：

```bash
./scripts/migrate_postgres.sh
```

脚本需要 `psql`，接受 PostgreSQL URI 与 API 使用的 `postgresql+psycopg://`
URI。它按文件编号执行 SQL，对同一数据库的并发调用串行化，任一语句失败会
回滚本次执行。空库与已有数据库使用同一入口。升级会取得表锁，建议在服务
启动前执行。

投递记录仍存于 `messages`。如果旧库仅用过核心实体建表脚本，原
`messages` 中的工作流消息实例会随表迁至 `workflow_messages`，保留原数据与外键。
历史记录归入默认租户，缺少环境的执行明细归入 `development`。

`./scripts/test_postgres_migrations.sh` 使用临时 PostgreSQL 实例验证初始化、
升级与重复执行，需要本机 PostgreSQL 服务端和客户端工具。

## 镜像构建

```bash
# 单独构建（上下文是仓库根目录）
docker build -f api/Dockerfile -t xw-notify-api .
docker build -f engine/Dockerfile -t xw-notify-engine .
```

- **api**：`python:3.13-slim`，先装 `api/requirements.txt` 再拷代码（依赖层
  缓存命中率高），`uvicorn` 起 `app.main:app`。
- **engine**：多阶段构建——`golang:1.23-alpine` 编译出静态二进制，运行层是
  `alpine:3.20` 加一个非 root 用户，二进制自带全部运行时依赖。

两个镜像都不携带任何密钥：队列地址、存储后端、回调密钥等全部走环境变量。

## 共享数据卷

引擎把执行明细、收件箱、偏好文件写到 `/data`，API 从同一目录读回
（`XW_DATA_DIR=/data`），compose 里两者挂同一个 `app_data` 卷。镜像在构建时
把 `/data` 预建为 `1777`：命名卷首次挂载会继承镜像里该目录的属主与权限，
而引擎（uid 10001）与 API（root）各用各的用户，谁先启动都不该把对方挡在
写入之外——这一条踩过一次：`/data` 归 root 时引擎全部投递报
`permission denied`，收件箱永远是空的，而容器状态看起来完全健康。

需要持久化跨重启的消息数据时，给 `app_data` 配置外部卷或宿主机绑定挂载即可，
镜像内的 `/data` 不保存任何必须随镜像分发的内容。

## 与进程部署的差异

| 项 | 容器 | 本机进程 |
| --- | --- | --- |
| 数据目录 | `/data` 共享卷 | `XW_DATA_DIR` 或默认 `api/data` |
| 队列 | `redis:6379`（compose 网络内） | `localhost:6379` |
| 引擎写、API 读 | 同一卷的同一路径 | 同一目录的同一路径 |
| 密钥 | 环境变量注入 | 环境变量 / 本地配置 |

功能语义完全一致：镜像只是换个方式把两个进程跑起来，接口、文件格式与
配置变量都没有容器专用的分支。

## 域名发布

`infra/release/compose.yml` 在宿主机的 `127.0.0.1:2027` 提供社区演示，
供 `xwp.xwd.info` 的反向代理接入。根路径打开社区首页，公开显示帖子。
`owner` 可发帖并查看社区概况，`guest` 可点赞和评论。API、Go 引擎和 Redis 均在
Compose 网络内。Redis 和通知文件存于命名卷，演示帖与创作者档案存于应用内存。

完整控制台由 API 托管，监听宿主机的 `127.0.0.1:2028`。用 SSH 转发后，
在 `http://127.0.0.1:2028/console/admin.html` 用 owner 登录，再进入工作台查看
工作流编排器与收件箱。控制台收件箱用带登录凭据的请求轮询。

```sh
ssh -NT -L 127.0.0.1:2028:127.0.0.1:2028 aly
```

### 服务器配置

部署用户需要 Docker、支持 `up --wait` 的 Compose、`flock` 和读取 GHCR
镜像的权限。私有镜像由该用户提前使用具有 `read:packages` 的凭据登录
`ghcr.io`。服务器端执行：

```sh
install -d -m 700 "$HOME/.xw-notify"
```

以 `infra/release/runtime.env.example` 为模板，在目录内保存权限为 `600` 的
`runtime.env`，填写独立的 owner 和 guest 口令。生成随机 API Key，填入
`XW_API_KEY`，并在 `XW_API_KEYS` 中配置同一值：

```dotenv
XW_API_KEYS='[{"key":"填入同一个 API Key","tenant":"default","environments":["development"]}]'
```

另生成独立的随机签名密钥填入 `XW_JWT_SECRET`（例如使用 `openssl rand -hex 32`）。

服务器无法访问 Docker Hub 时，可用 `XW_REDIS_IMAGE` 指定可达的 Redis 镜像。

反向代理转发到 `http://127.0.0.1:2027`。配置 TLS 后通过 HTTPS 传输登录凭据。

### GitHub 发布配置

在仓库的 `production` 环境添加以下配置：

| 类型 | 名称 | 值 |
| --- | --- | --- |
| Variable | `XW_DEPLOY_HOST` | SSH 可达的服务器主机名或 IP |
| Variable | `XW_DEPLOY_USER` | 部署用户 |
| Variable | `XW_DEPLOY_PORT` | SSH 端口，默认 22 |
| Variable | `XW_PUBLIC_URL` | 站点 URL，默认 `http://xwp.xwd.info` |
| Secret | `XW_DEPLOY_SSH_KEY` | 部署用户的 SSH 专用私钥 |
| Secret | `XW_DEPLOY_KNOWN_HOSTS` | 已核验的服务器主机公钥记录 |

`main` 的 CI 通过后，`release` 工作流发布带完整提交 SHA 的 API、引擎和演示
镜像。服务器按 SHA 保存 Compose 配置并串行更新，全部容器就绪后记录当前
SHA。容器就绪失败会尝试恢复上一版，公网探测在容器就绪后单独执行。

手动回滚时，在服务器上将部署脚本的参数设为目标 SHA。该版本的 Compose
文件须已保存在 `$HOME/.xw-notify/releases/<SHA>/compose.yml`。

```sh
sh apply.sh ghcr.io/xwd-ai/xw-p <SHA>
```
