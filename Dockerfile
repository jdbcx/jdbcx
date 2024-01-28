#
# Docker image for JDBCX
#

# Stage 1 - build minimal JRE
FROM eclipse-temurin:21-jdk-jammy AS jdk

RUN jlink --add-modules \
    java.base,java.compiler,java.logging,java.scripting,java.sql,java.transaction.xa,jdk.crypto.ec,jdk.crypto.cryptoki,jdk.httpserver \
    --output /min-jre --strip-debug --no-man-pages --no-header-files --verbose

# Stage 2 - build jdbcx
FROM ubuntu:jammy

# Maintainer
LABEL maintainer="zhicwu@gmail.com"

ARG PRQLC_VERSION=0.11.1
ARG JDBCX_VERSION=0.4.0

# Environment variables
ENV LANG="en_US.UTF-8" LANGUAGE="en_US:en" LC_ALL="en_US.UTF-8" TERM=xterm \
    JAVA_HOME="/app/openjdk" PATH="${PATH}:/app/openjdk/bin" \
    JDBCX_USER_ID=1000 JDBCX_USER_NAME=jdbcx JDBCX_VERSION=${JDBCX_VERSION:-0.4.0}

# Labels
LABEL os.dist=Ubuntu os.version=22.04 app.name=JDBCX app.version=${JDBCX_VERSION}

# Configure system(charset and timezone) and install ClickHouse
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y ca-certificates locales curl wget tzdata \
    && locale-gen en_US.UTF-8 \
    && wget -nv -O /tmp/prqlc.deb \
        https://github.com/PRQL/prql/releases/download/${PRQLC_VERSION}/prqlc_${PRQLC_VERSION}_$(arch | sed -e 's|aarch64|arm64|' -e 's|x86_64|amd64|').deb \
    && dpkg -i /tmp/prqlc.deb \
    && groupadd -r -g ${JDBCX_USER_ID} ${JDBCX_USER_NAME} \
    && useradd -r -Md /app -s /bin/bash -u ${JDBCX_USER_ID} -g ${JDBCX_USER_ID} ${JDBCX_USER_NAME} \
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
    && wget -nv -O /app/drivers/duckdb.LICENSE https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE \
    && wget -nv -O /app/drivers/mysql-connector-j.LICENSE \
        https://raw.githubusercontent.com/mysql/mysql-connector-j/release/8.x/LICENSE \
    && wget -nv -O /app/drivers/pgjdbc.LICENSE \
        https://raw.githubusercontent.com/pgjdbc/pgjdbc/master/LICENSE \
    && wget -nv -O /app/drivers/rhino.LICENSE \
        https://raw.githubusercontent.com/mozilla/rhino/master/LICENSE.txt \
    && wget -nv -P /app/drivers/ \
        https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.11/slf4j-api-2.0.11.jar \
        https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.11/slf4j-simple-2.0.11.jar \
        https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-http.jar \
        https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar \
        https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.9.2/duckdb_jdbc-0.9.2.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar \
        https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.45.0.0/sqlite-jdbc-3.45.0.0.jar \
        https://repo1.maven.org/maven2/org/mozilla/rhino/1.7.14/rhino-1.7.14.jar \
        https://repo1.maven.org/maven2/org/mozilla/rhino-engine/1.7.14/rhino-engine-1.7.14.jar \
    && apt-get clean \
	&& rm -rf /tmp/* /var/cache/debconf /var/lib/apt/lists/*

# Use custom configuration
COPY --from=jdk /min-jre /app/openjdk
COPY --chown=root:root docker/ /

RUN chmod +x /*.sh

USER jdbcx

ENTRYPOINT [ "/entrypoint.sh" ]

VOLUME [ "/app/drivers" ]

# bridge server
EXPOSE 8080

WORKDIR /app
