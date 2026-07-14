FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir aiohttp redis
COPY main.py .
ENTRYPOINT ["python", "main.py"]
