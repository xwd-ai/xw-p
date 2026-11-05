# 引擎镜像：Go 执行引擎，多阶段构建。
#
# 构建：docker build -f engine/Dockerfile -t xw-notify-engine .
# 队列地址、存储路径、回调配置全部走环境变量（XW_REDIS_ADDR / XW_INBOX_FILE 等）。
FROM golang:1.23-alpine AS build

WORKDIR /src
COPY engine/go.mod engine/go.sum ./
RUN go mod download
COPY engine/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/xw-engine ./cmd/engine

FROM alpine:3.20

RUN adduser -D -u 10001 engine
COPY --from=build /out/xw-engine /usr/local/bin/xw-engine

# /data 与 API 容器共享：执行明细、收件箱、偏好文件都写在这里。
# 目录预建并放开写权限：命名卷首次挂载会继承镜像里该目录的属主/权限，
# 引擎与 API 两个容器各用各的用户，谁先起都不该把对方挡在门外。
RUN mkdir -p /data && chmod 1777 /data
ENV XW_INBOX_FILE=/data/inbox.jsonl \
    XW_EXECUTION_FILE=/data/executions.jsonl \
    XW_PREFERENCES_FILE=/data/preferences.jsonl \
    XW_CALLBACK_AUDIT_FILE=/data/callback_audit.jsonl \
    XW_CALLBACK_EVENT_FILE=/data/callback_events.jsonl
VOLUME ["/data"]

USER engine
EXPOSE 8081
ENTRYPOINT ["xw-engine"]
