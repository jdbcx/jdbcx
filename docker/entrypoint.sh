#!/bin/bash

set -e

: ${DRIVER_OPTS:=""}
: ${SERVER_OPTS:=""}

get_classpath() {
    echo -n "$(find `pwd`/lib -type f -name '*.jar' -exec printf '{}:' \;)jdbcx.jar:."
}

run_cli() {
    java ${DRIVER_OPTS} -cp $(get_classpath) io.github.jdbcx.Main "$@"
}

start_server() {
    java --add-opens=java.base/java.nio=ALL-UNNAMED \
        ${SERVER_OPTS} -cp $(get_classpath) io.github.jdbcx.server.BridgeServer
}

# start clickhouse server
if [ "$#" == "1" ] && [ "$1" == "server" ]; then
    start_server
else
    run_cli "$@"
fi
