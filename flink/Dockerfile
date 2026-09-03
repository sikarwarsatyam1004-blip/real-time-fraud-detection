FROM flink:2.1.3-scala_2.12-java17

USER root

RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-dev \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

RUN python3 -m pip install \
    --no-cache-dir \
    --upgrade pip setuptools wheel

RUN python3 -m pip install \
    --no-cache-dir \
    --break-system-packages \
    apache-flink==2.1.3

RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar

USER flink