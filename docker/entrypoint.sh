#!/bin/bash

set -e

: ${BASE_DIR:="${JDBCX_HOME:-/app}"}

: ${DRIVER_OPTS:=""}
: ${SERVER_OPTS:=""}

get_classpath() {
    echo -n "$(find ${BASE_DIR}/lib -type f -name '*.jar' -exec printf '{}:' \;)${BASE_DIR}/jdbcx.jar:."
}

run_cli() {
    java --add-opens=java.base/java.nio=ALL-UNNAMED -cp $(get_classpath) \
        ${DRIVER_OPTS} io.github.jdbcx.Main "$@"
}

run_server() {
    local log_conf_file="${BASE_DIR}/simplelogger.properties"
    if [ ! -f ${log_conf_file} ]; then
        cat << EOF > ${log_conf_file}
org.slf4j.simpleLogger.defaultLogLevel=info
org.slf4j.simpleLogger.log.io.github.jdbcx=debug
org.slf4j.simpleLogger.log.io.github.jdbcx.internal=warn
org.slf4j.simpleLogger.showDateTime=true
org.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd HH:mm:ss:SSS Z
org.slf4j.simpleLogger.showThreadName=true
org.slf4j.simpleLogger.showLogName=true
org.slf4j.simpleLogger.showShortLogName=true
org.slf4j.simpleLogger.logFile=System.out
org.slf4j.simpleLogger.cacheOutputStream=true
EOF
    fi

    java --add-opens=java.base/java.nio=ALL-UNNAMED -cp $(get_classpath) \
        ${SERVER_OPTS} io.github.jdbcx.server.BridgeServer
}

if [ "$#" == "1" ] && [ "$1" == "server" ]; then
    run_server
else
    run_cli "$@"
fi
