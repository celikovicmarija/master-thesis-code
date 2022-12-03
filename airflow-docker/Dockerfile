

# Base Image
FROM apache/airflow:2.4.3
COPY requirements.txt /requirements.txt
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

RUN curl -L -o /mysql-connector-java-8.0.30.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
COPY mysql-connector-java-8.0.30.jar /opt/spark/jars
ENV CLASSPATH=/mysql-connector-java-8.0.30.jar:${CLASSPATH}

USER airflow

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt