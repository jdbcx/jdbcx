#!/bin/bash

set -e

: ${JDBCX_OPTS:=""}
: ${SERVER_OPTS:="-Dserver.host=0.0.0.0"}

run_cli() {
    java ${JDBCX_OPTS} -cp jdbcx.jar io.github.jdbcx.Main "$@"
}

start_server() {
	java ${SERVER_OPTS} -cp jdbcx.jar:slf4j-api.jar:slf4j-simple.jar io.github.jdbcx.server.BridgeServer
}

# start clickhouse server
if [ "$#" == "1" ] && [ "$1" == "server" ]; then
    start_server
else
	run_cli "$@"
fi
