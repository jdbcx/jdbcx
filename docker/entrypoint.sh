#!/bin/bash

set -e

: ${DRIVER_OPTS:=""}
: ${SERVER_OPTS:=""}

get_classpath() {
    echo -n "$(find `pwd`/lib -type f -name '*.jar' -exec printf '{}:' \;)jdbcx.jar:."
}

run_cli() {
    java --add-opens=java.base/java.nio=ALL-UNNAMED -cp $(get_classpath) \
        ${DRIVER_OPTS} io.github.jdbcx.Main "$@"
}

start_server() {
    [ ! -f simplelogger.properties ] && cp -fv simplelogger.properties.disabled simplelogger.properties
    java --add-opens=java.base/java.nio=ALL-UNNAMED -cp $(get_classpath) \
        ${SERVER_OPTS} io.github.jdbcx.server.BridgeServer
}

# start clickhouse server
if [ "$#" == "1" ] && [ "$1" == "server" ]; then
    start_server
else
    run_cli "$@"
fi
