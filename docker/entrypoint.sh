#!/bin/bash

set -e

: ${VERBOSE:="false"}

run_cli() {
    java -Djdbcx.config.path=${JDBCX_CONFIG} -Dverbose=${VERBOSE} -jar jdbcx.jar "$@"
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
