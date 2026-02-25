FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["python", "-u", "app.py", "--host", "0.0.0.0", "--port", "8000", "--port-tries", "1"]
