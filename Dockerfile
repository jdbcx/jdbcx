#
# Docker image for JDBCX
#

# Stage 1 - build minimal JRE
FROM eclipse-temurin:21-jdk-jammy AS jdk

RUN jlink --add-modules \
    java.base,java.compiler,java.logging,java.naming,java.scripting,java.sql,java.transaction.xa,jdk.crypto.ec,jdk.crypto.cryptoki,jdk.httpserver,jdk.management,jdk.unsupported \
    --output /min-jre --strip-debug --no-man-pages --no-header-files --verbose

# Stage 2 - build jdbcx
FROM ubuntu:jammy

# Maintainer
LABEL maintainer="zhicwu@gmail.com"

ARG PRQLC_VERSION=0.11.4
ARG JDBCX_VERSION=0.5.0

# Environment variables
ENV LANG="en_US.UTF-8" LANGUAGE="en_US:en" LC_ALL="en_US.UTF-8" TERM=xterm \
    JAVA_HOME="/app/openjdk" PATH="${PATH}:/app/openjdk/bin" \
    JDBCX_USER_ID=1000 JDBCX_USER_NAME=jdbcx JDBCX_VERSION=${JDBCX_VERSION:-0.5.0}

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
    && apt-get clean \
    && rm -rf /tmp/* /var/cache/debconf /var/lib/apt/lists/*

# Use custom configuration
COPY --chown=${JDBCX_USER_NAME}:${JDBCX_USER_NAME} docker/ /

WORKDIR /app

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
    && wget -nv -O ./drivers/duckdb.LICENSE https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE \
    && wget -nv -O ./drivers/mysql-connector-j.LICENSE \
        https://raw.githubusercontent.com/mysql/mysql-connector-j/release/8.x/LICENSE \
    && wget -nv -O ./drivers/pgjdbc.LICENSE \
        https://raw.githubusercontent.com/pgjdbc/pgjdbc/master/LICENSE \
    && wget -nv -O ./drivers/rhino.LICENSE \
        https://raw.githubusercontent.com/mozilla/rhino/master/LICENSE.txt \
    && wget -nv -P ./drivers/ \
        https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-http.jar \
        https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar \
        https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.10.2/duckdb_jdbc-0.10.2.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.45.3.0/sqlite-jdbc-3.45.3.0.jar \
    && rm -fv ./*.tar.gz /tmp/*

USER jdbcx

RUN for ext in arrow aws azure fts httpfs json mysql parquet postgres sqlite vss; \
    do ./openjdk/bin/java -Dverbose=true -DnoProperties=true -jar jdbcx.jar 'jdbcx:duckdb:' "INSTALL $ext" || true; done

ENTRYPOINT [ "/entrypoint.sh" ]

VOLUME [ "/app/drivers" ]

# bridge server
EXPOSE 8080
