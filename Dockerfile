#
# Docker image for JDBCX
#

# Stage 1 - build minimal JRE
FROM eclipse-temurin:21-jdk-noble AS jdk

RUN jlink --add-modules \
    java.base,java.compiler,java.logging,java.naming,java.scripting,java.sql,java.transaction.xa,jdk.crypto.ec,jdk.crypto.cryptoki,jdk.dynalink,jdk.httpserver,jdk.management,jdk.unsupported \
    --output /min-jre --strip-debug --no-man-pages --no-header-files --verbose

# Stage 2 - build jdbcx
FROM ubuntu:noble

# Maintainer
LABEL maintainer="zhicwu@gmail.com"

ARG PRQLC_VERSION=0.13.4
ARG JDBCX_VERSION=0.6.0

# Environment variables
ENV LANG="en_US.UTF-8" LANGUAGE="en_US:en" LC_ALL="en_US.UTF-8" TERM=xterm \
    JDBCX_HOME="/app" JAVA_HOME="/app/openjdk" PATH="${PATH}:/app/openjdk/bin" \
    JDBCX_USER_ID=2025 JDBCX_USER_NAME=jdbcx JDBCX_VERSION=${JDBCX_VERSION:-0.6.0} \
    CLICKHOUSE_DRIVER_VERSION=0.4.6 DUCKDB_DRIVER_VERSION=1.2.2.0 MYSQL_DRIVER_VERSION=9.2.0 \
    OPENSEARCH_DRIVER_VERSION=1.4.0.1 POSTGRES_DRIVER_VERSION=42.7.5 SQLITE_DRIVER_VERSION=3.49.1.0

# Labels
LABEL os.dist=Ubuntu os.version=24.04 app.name=JDBCX app.version=${JDBCX_VERSION}

# Configure system(charset and timezone) and install ClickHouse
RUN apt update \
    && apt upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends -y \
        ca-certificates locales curl ssh-client wget tzdata iputils-ping net-tools \
    && locale-gen en_US.UTF-8 \
    && wget -nv -O /tmp/prqlc.deb \
        https://github.com/PRQL/prql/releases/download/${PRQLC_VERSION}/prqlc_${PRQLC_VERSION}_$(arch | sed -e 's|aarch64|arm64|' -e 's|x86_64|amd64|').deb \
    && dpkg -i /tmp/prqlc.deb \
    && groupadd -r -g ${JDBCX_USER_ID} ${JDBCX_USER_NAME} \
    && useradd -r -Md ${JDBCX_HOME} -s /bin/bash -u ${JDBCX_USER_ID} -g ${JDBCX_USER_ID} ${JDBCX_USER_NAME} \
    && echo 13 > /etc/timezone \
    && echo 33 >> /etc/timezone \
    && cat /etc/timezone | dpkg-reconfigure -f noninteractive tzdata \
    && apt-get clean \
    && rm -rf /tmp/* /var/cache/debconf /var/lib/apt/lists/*

# Use custom configuration
COPY --chown=${JDBCX_USER_NAME}:${JDBCX_USER_NAME} docker/ /

WORKDIR ${JDBCX_HOME}

COPY --from=jdk /min-jre ./openjdk

# Download JDBCX conditionally
RUN if [ -f jdbcx-server-*.jar ]; then echo "Skip downloading"; \
    else wget -nv https://github.com/jdbcx/jdbcx/releases/download/v${JDBCX_VERSION}/jdbcx-server-${JDBCX_VERSION}.jar \
        https://github.com/jdbcx/jdbcx/releases/download/v${JDBCX_VERSION}/jdbcx-server-${JDBCX_VERSION}-dependencies.tar.gz \
        https://github.com/jdbcx/jdbcx/releases/download/v${JDBCX_VERSION}/LICENSE \
        https://github.com/jdbcx/jdbcx/releases/download/v${JDBCX_VERSION}/NOTICE; fi

RUN chmod +x /*.sh \
    && ln -s jdbcx-server-*.jar jdbcx.jar \
    && tar -zxvf jdbcx-server-*-dependencies.tar.gz --strip 1 -C lib/ \
    && wget -nv -O ./lib/rhino.LICENSE https://raw.githubusercontent.com/mozilla/rhino/master/LICENSE.txt \
    && mkdir -p drivers/clickhouse drivers/duckdb drivers/mysql drivers/opensearch drivers/postgres drivers/sqlite \
    && wget -nv -P ./drivers/clickhouse/ \
        https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_DRIVER_VERSION}/clickhouse-jdbc-${CLICKHOUSE_DRIVER_VERSION}-http.jar \
    && wget -nv -P ./drivers/duckdb/ https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE \
        https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/${DUCKDB_DRIVER_VERSION}/duckdb_jdbc-${DUCKDB_DRIVER_VERSION}.jar \
    && wget -nv -P ./drivers/mysql/ https://raw.githubusercontent.com/mysql/mysql-connector-j/release/9.x/LICENSE \
        https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_DRIVER_VERSION}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar \
    && wget -nv -P ./drivers/opensearch/ \
        https://repo1.maven.org/maven2/org/opensearch/driver/opensearch-sql-jdbc/${OPENSEARCH_DRIVER_VERSION}/opensearch-sql-jdbc-${OPENSEARCH_DRIVER_VERSION}.jar \
    && wget -nv -P ./drivers/postgres/ https://raw.githubusercontent.com/pgjdbc/pgjdbc/master/LICENSE \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar \
    && wget -nv -P ./drivers/sqlite/ \
        https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/${SQLITE_DRIVER_VERSION}/sqlite-jdbc-${SQLITE_DRIVER_VERSION}.jar \
    && rm -fv ./drivers/.gitkeep ./lib/.gitkeep ./*.tar.gz /tmp/* \
    && cd drivers \
    && ln -s clickhouse/clickhouse-jdbc-${CLICKHOUSE_DRIVER_VERSION}-http.jar clickhouse-jdbc.jar \
    && ln -s duckdb/duckdb_jdbc-${DUCKDB_DRIVER_VERSION}.jar duckdb-jdbc.jar \
    && ln -s mysql/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar mysql-jdbc.jar \
    && ln -s opensearch/opensearch-sql-jdbc-${OPENSEARCH_DRIVER_VERSION}.jar opensearch-jdbc.jar \
    && ln -s postgres/postgresql-${POSTGRES_DRIVER_VERSION}.jar postgresql-jdbc.jar \
    && ln -s sqlite/sqlite-jdbc-${SQLITE_DRIVER_VERSION}.jar sqlite-jdbc.jar

USER jdbcx

RUN for ext in arrow aws azure fts httpfs json mysql parquet postgres sqlite vss; \
    do ./openjdk/bin/java -Dverbose=true -cp jdbcx.jar io.github.jdbcx.Main 'jdbcx:duckdb:' "INSTALL $ext" || true; done

ENTRYPOINT [ "/entrypoint.sh" ]

VOLUME [ "${JDBCX_HOME}/drivers" ]

# bridge server
EXPOSE 8080
