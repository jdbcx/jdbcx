#!/bin/bash

set -e

: ${DRIVER_OPTS:=""}
: ${SERVER_OPTS:=""}

run_cli() {
    java ${DRIVER_OPTS} -cp jdbcx.jar io.github.jdbcx.Main "$@"
}

start_server() {
    local classpath="$(find /app/lib -type f -name '*.jar' -exec printf '{}:' \;)jdbcx.jar:."
    java --add-opens=java.base/java.nio=ALL-UNNAMED \
        ${SERVER_OPTS} -cp ${classpath} io.github.jdbcx.server.BridgeServer
}

# start clickhouse server
if [ "$#" == "1" ] && [ "$1" == "server" ]; then
    start_server
else
    run_cli "$@"
fi
