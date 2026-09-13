# One image for every pipeline component: Spark jobs, dlt pipelines,
# Kafka producer/consumers, dbt, Bruin and the Streamlit dashboard.
# Kestra launches task containers from this same image.
FROM python:3.12-slim-bookworm

ARG BRUIN_VERSION=0.11.757
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONPATH=/app/src \
    TZ=UTC

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates git procps \
 && rm -rf /var/lib/apt/lists/*

# Bruin CLI (Module 5 - data platforms)
RUN curl -fsSL -o /tmp/bruin.tar.gz \
      "https://github.com/bruin-data/bruin/releases/download/v${BRUIN_VERSION}/bruin_Linux_x86_64.tar.gz" \
 && tar -xzf /tmp/bruin.tar.gz -C /usr/local/bin bruin \
 && chmod +x /usr/local/bin/bruin && rm /tmp/bruin.tar.gz

WORKDIR /app
# requirements.lock is generated from requirements.txt with `uv pip compile` (see Makefile: lock)
COPY docker/pipelines/requirements.lock /tmp/requirements.lock
RUN pip install uv==0.9.* && uv pip install --system --no-cache -r /tmp/requirements.lock

# Bake the S3A + Postgres JDBC jars into Spark so jobs run fully offline.
# hadoop-aws must match the Hadoop version bundled with PySpark.
COPY docker/pipelines/fetch_spark_jars.py /tmp/fetch_spark_jars.py
RUN python /tmp/fetch_spark_jars.py && rm -rf /tmp/ivy /tmp/fetch_spark_jars.py

COPY src ./src
COPY dbt ./dbt
COPY bruin ./bruin
COPY dashboard ./dashboard
COPY kestra ./kestra
COPY tests ./tests
COPY pyproject.toml ./
# Bruin refuses asset files with unusual permissions (e.g. 777 from Windows checkouts)
RUN find /app/bruin -type f -exec chmod 644 {} + && chmod 755 /app/bruin/run.sh

# dbt packages/parse artefacts are not needed at build time; profiles come from env vars
ENV DBT_PROFILES_DIR=/app/dbt \
    DBT_PROJECT_DIR=/app/dbt

CMD ["bash"]
