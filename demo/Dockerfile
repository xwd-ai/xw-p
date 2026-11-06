FROM python:3.13-slim

WORKDIR /app
COPY api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && useradd --system --uid 10002 demo

COPY demo/__init__.py demo/app.py demo/community.py ./demo/
COPY dashboard/community ./dashboard/community
COPY sdk/python/xw_notify ./sdk/python/xw_notify
ENV PYTHONUNBUFFERED=1
USER demo
EXPOSE 8100
CMD ["uvicorn", "demo.app:app", "--host", "0.0.0.0", "--port", "8100"]
