# Build stage - compile the Flink job with Maven
FROM docker.io/library/maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src/ src/
RUN mvn package -DskipTests -B

# Runtime stage - standard Flink image with Iceberg dependencies
FROM docker.io/library/flink:2.0.1-java17

# Download Iceberg + Hadoop + JDBC jars into lib/ (system classpath).
RUN curl -fSL -o /opt/flink/lib/iceberg-flink-runtime.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.0/1.10.1/iceberg-flink-runtime-2.0-1.10.1.jar && \
    curl -fSL -o /opt/flink/lib/flink-connector-kafka.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar && \
    curl -fSL -o /opt/flink/lib/kafka-clients.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar && \
    curl -fSL -o /opt/flink/lib/iceberg-aws-bundle.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar && \
    curl -fSL -o /opt/flink/lib/hadoop-client-api.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar && \
    curl -fSL -o /opt/flink/lib/hadoop-client-runtime.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar && \
    curl -fSL -o /opt/flink/lib/hadoop-common.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.3/hadoop-common-3.4.3.jar && \
    curl -fSL -o /opt/flink/lib/postgresql.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar

COPY --from=builder /build/target/flink-osquery-job-1.0.0.jar /opt/flink/lib/flink-osquery-job.jar
