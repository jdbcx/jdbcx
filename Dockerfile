#
# Minimal JRE Docker Image for JDBCX
#

# Stage 1 - build minimal JRE
FROM eclipse-temurin:21-jdk-noble AS jdk

RUN jlink --add-modules \
    java.base,java.compiler,java.logging,java.naming,java.scripting,java.sql,java.transaction.xa,jdk.crypto.ec,jdk.crypto.cryptoki,jdk.dynalink,jdk.httpserver,jdk.management,jdk.unsupported \
    --output /min-jre --strip-debug --no-man-pages --no-header-files --verbose

# Stage 2 - build jdbcx
FROM jdbcx/jdbcx-base:latest

COPY --from=jdk /min-jre ./openjdk

USER jdbcx

RUN for ext in arrow aws azure fts httpfs json mysql parquet postgres sqlite vss; \
    do ./openjdk/bin/java -Dverbose=true -cp jdbcx.jar io.github.jdbcx.Main 'jdbcx:duckdb:' "INSTALL $ext" || true; done
