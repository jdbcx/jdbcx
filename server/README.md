# Bridge Server

A web server that exposes remote datasets based on specified data formats and compression algorithms. When used with the JDBCX driver, it greatly simplifies federated queries. Here's a diagram showing how it works.
![image](https://github.com/jdbcx/jdbcx/assets/4270380/4dc3b215-16d9-4d40-8b94-778f68529919)


## Quick Start

Spinning up a bridge server locally for testing is straightforward.

```bash
# start the bridge server in background
docker run --rm -it -p8080:8080 --name bridge -d jdbcx/jdbcx server

# check server configuration
curl 'http://localhost:8080/config'

# execute query and get result in CSV format
curl -H 'X-Query-Mode: DIRECT' --data "select 1 as a, '2' as b" 'http://localhost:8080/'
# same as above but uses query parameters
curl --data "select 1 as a, '2' as b" 'http://localhost:8080/?m=d'

# JSONL format with gzip compression
curl -H 'Accept: application/jsonl' -H 'Accept-Encoding: gzip' -H 'X-Query-Mode: DIRECT' \
    --data "select 1 as a, '2' as b" 'http://localhost:8080/' --output -
# same as above but uses query parameters
curl --data "select 1 as a, '2' as b" 'http://localhost:8080/1.jsonl.gz?m=d' --output -

# shutdown and remove the bridge server
docker stop bridge
```

## Configuration

### Server

Bridge server uses the same `~/.jdbcx/config.properties` file for configuration. Please refer to [this](/docker/app/.jdbcx/config.properties) used in docker image, and named database connections configured at [here](/docker/app/.jdbcx/db).

| Key | Value | Remark |
| --- | ----- | ------ |
| jdbcx.server.auth | Defaults to `false` | Whether to enable authentication |
| jdbcx.server.host | Defaults to `0.0.0.0` | Listen address of the server |
| jdbcx.server.port | Defaults to `8080` | Listen port of the server |
| jdbcx.server.context | Defaults to `/` | Web context of the server, must be ends with `/` |
| jdbcx.server.url | Defaults to `http://127.0.0.1:8080/` | Bridge server URL that can be accessed remotely, must be ends with `/` |

### Datasource

`datasource.properties` defines the default database used by bridge server. Please refer to [this](/docker/app/datasource.properties) used in docker image.

### Access Control

`acl.properties` must be specified when authentication is enabled. Please refer to [this](/docker/app/acl.properties) used in docker image. ACL will be ignored when `jdbcx.server.auth` is set to `false`.

| Key | Value | Remark |
| --- | ----- | ------ |
| _prefix_.token | Access token | |
| _prefix_.hosts | Authorized host names | Comma separated host names, it's recommended to use `.ips` instead |
| _prefix_.ips | Authorized IP addreses | Comma separated IP addresses or IP ranges |
