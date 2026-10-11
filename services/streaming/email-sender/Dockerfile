FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# -u: unbuffered, so "emailed ..." lines show up in `docker logs` live.
CMD ["python", "-u", "main.py"]
