# Dataproc supplies Spark and Java. Keep the project Python runtime at 3.15.
# Resolved by Cloud Build 1e7a033e-7338-4cf1-85f9-77574c5af3bf.
FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928
RUN apt-get update && apt-get install -y --no-install-recommends procps tini libjemalloc2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1099 spark && useradd -u 1099 -g 1099 -m spark
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
USER spark
