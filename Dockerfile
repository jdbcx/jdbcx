#
# Docker image for JDBCX
#

# Stage 1 - build prqlc
#FROM rust:1.70 AS builder

#RUN cargo install prqlc

# Stage 2 - build jdbcx
FROM eclipse-temurin:17-jdk-jammy

# Maintainer
LABEL maintainer="zhicwu@gmail.com"

ARG JDBCX_VERSION=0.3.0

# Environment variables
ENV LANG="en_US.UTF-8" LANGUAGE="en_US:en" LC_ALL="en_US.UTF-8" TERM=xterm \
    JDBCX_USER_ID=1000 JDBCX_USER_NAME=jdbcx JDBCX_VERSION=${JDBCX_VERSION:-0.3.0}

# Labels
LABEL os.dist=Ubuntu os.version=22.04 app.name="JDBCX" app.version="$JDBCX_VERSION"

# Configure system(charset and timezone) and install ClickHouse
RUN locale-gen en_US.UTF-8 \
    && groupadd -r -g $JDBCX_USER_ID $JDBCX_USER_NAME \
    && useradd -r -Md /app -s /bin/bash -u $JDBCX_USER_ID -g $JDBCX_USER_ID $JDBCX_USER_NAME \
    && echo 13 > /etc/timezone \
    && echo 33 >> /etc/timezone \
    && cat /etc/timezone | dpkg-reconfigure -f noninteractive tzdata \
    && mkdir -p /app/drivers \
    && chown -R jdbcx:jdbcx /app \
    && wget -nv -P /app \
        https://github.com/jdbcx/jdbcx/releases/download/v${JDBCX_VERSION}/jdbcx-driver-${JDBCX_VERSION}.jar \
        https://github.com/jdbcx/jdbcx/releases/download/v${JDBCX_VERSION}/LICENSE \
        https://github.com/jdbcx/jdbcx/releases/download/v${JDBCX_VERSION}/NOTICE \
    && ln -s /app/jdbcx-driver-${JDBCX_VERSION}.jar /app/jdbcx.jar \
    && wget -nv -O /app/drivers/duckdb.LICENSE https://github.com/duckdb/duckdb/raw/master/LICENSE \
    && wget -nv -O /app/drivers/mysql-connector-j.LICENSE \
        https://raw.githubusercontent.com/mysql/mysql-connector-j/release/8.0/LICENSE \
    && wget -nv -O /app/drivers/pgjdbc.LICENSE \
        https://raw.githubusercontent.com/pgjdbc/pgjdbc/master/LICENSE \
    && wget -nv -O /app/drivers/rhino.LICENSE \
        https://raw.githubusercontent.com/mozilla/rhino/master/LICENSE.txt\
    && wget -nv -P /app/drivers/ \
        https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-http.jar \
        https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.1.0/mysql-connector-j-8.1.0.jar \
        https://repo1.maven.org/maven2/org/apache/derby/derby/10.14.2.0/derby-10.14.2.0.jar \
        https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.8.1/duckdb_jdbc-0.8.1.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
        https://repo1.maven.org/maven2/org/mozilla/rhino/1.7.14/rhino-1.7.14.jar \
        https://repo1.maven.org/maven2/org/mozilla/rhino-engine/1.7.14/rhino-engine-1.7.14.jar

# Use custom configuration
#COPY --from=builder /usr/local/cargo/bin/prqlc /usr/local/bin/prqlc
COPY --chown=root:root docker/ /

RUN chmod +x /*.sh /usr/local/bin/*

USER jdbcx

ENTRYPOINT [ "/entrypoint.sh" ]

VOLUME [ "/app/drivers" ]

# 8080 - reserved for bridge server
EXPOSE 8080

WORKDIR /app
