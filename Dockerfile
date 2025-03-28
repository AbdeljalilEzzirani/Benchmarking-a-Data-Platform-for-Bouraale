FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    openjdk-11-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN pip3 install \
    polars \
    duckdb \
    dask[complete] \
    pyspark \
    matplotlib \
    seaborn \
    pyarrow \
    delta-spark \
    apache-beam[interactive]  # For ORC support

RUN useradd -m -s /bin/bash abdeljalil
USER abdeljalil
WORKDIR /home/abdeljalil/app

COPY --chown=abdeljalil:abdeljalil . /home/abdeljalil/app
COPY --chown=abdeljalil:abdeljalil data_sets/ /home/abdeljalil/app/data_sets/

CMD ["bash"]