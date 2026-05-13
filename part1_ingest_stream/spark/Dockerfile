FROM apache/spark:3.4.0

USER root

# ── Sistem bağımlılıkları ─────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        curl \
        netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# ── Python bağımlılıkları ─────────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# ── Spark Kafka + Delta Lake JAR'larını önceden indir ────────────────
# (spark-submit --packages ile ilk çalıştırmada otomatik indirilir, ancak
#  internetsiz / yavaş ortamlarda hazır olması için pre-fetch ediyoruz)
ENV SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0"
RUN /opt/spark/bin/spark-shell --packages "${SPARK_PACKAGES}" \
        --conf "spark.jars.ivy=/opt/spark/.ivy2" \
        -e ":quit" 2>/dev/null || true

# ── MLflow ve Jupyter portları ───────────────────────────────────────
EXPOSE 4040 8888

# ── Çalışma dizini ───────────────────────────────────────────────────
WORKDIR /workspace

USER spark
