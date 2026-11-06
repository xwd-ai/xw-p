# API 服务镜像：FastAPI + uvicorn。
#
# 构建：docker build -f api/Dockerfile -t xw-notify-api .
# 运行所需的队列/存储地址全部走环境变量（XW_BUS / XW_REDIS_URL / XW_STORAGE 等），
# 镜像本身不携带任何密钥。
FROM python:3.13-slim

WORKDIR /app

# 先装依赖再拷代码：依赖层变化频率远低于代码，缓存命中率高。
COPY api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY api/app ./app
COPY dashboard/*.html dashboard/*.css dashboard/*.js ./console/
COPY dashboard/community ./console/community

# 数据目录统一挂载点：XW_DATA_DIR 指向它，收件箱/执行明细/偏好等 JSONL 都落在这里，
# 与引擎容器共享同一个卷即可完成「引擎写、API 读」。
# 目录预建并放开写权限：命名卷首次挂载会继承这里设定，两个容器各用各的用户，
# 谁先起都不该把对方挡在门外。
RUN mkdir -p /data && chmod 1777 /data
ENV XW_DATA_DIR=/data \
    PYTHONUNBUFFERED=1
VOLUME ["/data"]

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
