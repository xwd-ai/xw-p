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
