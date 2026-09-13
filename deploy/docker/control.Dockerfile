FROM python:3.14-slim
WORKDIR /app
COPY harness/ harness/
CMD ["sleep", "infinity"]
