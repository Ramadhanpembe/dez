# Stage 1: let Maven resolve the connector JARs and all their dependencies
FROM maven:3.9-eclipse-temurin-11 AS libs
WORKDIR /build
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q dependency:copy-dependencies \
      -DoutputDirectory=/out \
      -DincludeScope=runtime \
      -DexcludeArtifactIds=slf4j-api

# Stage 2: your Flink image
FROM flink:2.3.0-java11

USER root

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends build-essential python3 python3-pip python3-dev wget && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python

ENV PIP_BREAK_SYSTEM_PACKAGES=1
RUN --mount=type=cache,target=/root/.cache/pip \
    pip3 install apache-flink==2.3.0

COPY --from=libs --chown=flink:flink /out/*.jar /opt/flink/lib/

USER flink