#!/bin/bash

set -e

: ${JDBCX_OPTS:=""}

run_cli() {
    java ${JDBCX_OPTS} -jar jdbcx.jar "$@"
}

start_server() {
	echo "Bridge server is currently not available..."
}

# start clickhouse server
if [ "$#" == "1" ] && [ "$1" == "server" ]; then
    start_server
else
	run_cli "$@"
fi
