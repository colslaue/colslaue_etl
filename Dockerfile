FROM python:3.9

WORKDIR /app
ENV PYTHONPATH=/app
ENTRYPOINT []
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "-m", "workers.tasks"]