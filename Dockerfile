FROM python:3.12-slim
RUN pip install --no-cache-dir kazoo boto3
WORKDIR /app
COPY coordinator.py .
CMD ["python", "-u", "coordinator.py"]
