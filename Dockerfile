FROM apache/airflow:2.9.2

USER root
RUN apt-get update
RUN apt install -y default-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get install -y wget
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
         
RUN wget https://downloads.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    mkdir -p /opt/spark && \
    tar -xvf spark-3.5.4-bin-hadoop3.tgz -C /opt/spark && \
    rm spark-3.5.4-bin-hadoop3.tgz

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark/spark-3.5.4-bin-hadoop3
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

COPY requirements.txt /requirements.txt
RUN chmod 777 /requirements.txt

USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
