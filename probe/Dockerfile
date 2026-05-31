FROM python:3.12-slim

WORKDIR /probe

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY probe.py .

EXPOSE 8000

CMD ["python", "probe.py"]
