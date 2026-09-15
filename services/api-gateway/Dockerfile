# api-gateway（多阶段构建；构建上下文为仓库根目录）
# 构建示例：docker build -f services/api-gateway/Dockerfile -t hunter/api-gateway:dev .
FROM python:3.12-slim AS builder
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /build
COPY common/python ./common/python
COPY services/api-gateway ./service
RUN pip install --prefix=/install ./common/python ./service

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=builder /install /usr/local
WORKDIR /app
COPY --from=builder /build/service/app ./app
EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
