FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DB_PATH=/data/batches.db

WORKDIR /srv

# 先装依赖，利用层缓存
COPY app/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 再拷应用代码
COPY app/ ./app/

# SQLite 数据目录（WAL 模式下需可写）
RUN mkdir -p /data && \
    useradd --create-home --uid 10001 appuser && \
    chown -R appuser:appuser /data /srv
USER appuser

EXPOSE 8000

# Compose 通过 command 覆盖为先初始化数据库再启动
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
