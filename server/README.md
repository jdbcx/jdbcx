# Bridge Server

A web server that exposes remote datasets based on specified data formats and compression algorithms. When used with the JDBCX driver, it greatly simplifies federated queries. Here's a diagram showing how it works.
![image](https://github.com/jdbcx/jdbcx/assets/4270380/4dc3b215-16d9-4d40-8b94-778f68529919)

## Quick Start

Docker is the easiest way to start with.

```bash
# start the bridge server in background
docker run --rm -it -p8080:8080 --name bridge -d jdbcx/jdbcx server

# check server configuration
curl http://localhost:8080/config

# get prometheus metrics for monitoring
curl http://localhost:8080/metrics

# execute query and get result in CSV format, without compression
curl -d "select 1 as a, '2' as b" http://localhost:8080/query
# same query, using GET instead of POST
curl --get --data-urlencode "q=select 1 as a, '2' as b" http://localhost:8080/query
# CSV without header
curl -d "select 1 as a, '2' as b" 'http://localhost:8080/query/unique-id.csv?header=false'

# execute dynamic query and get results in JSONL format with gzip compression
curl -d "select {{ script: [1,2,3] }} n, '{{script:['a','b']}}' s" \
    http://localhost:8080/query/unique-id.jsonl.gz | gzip -d
# same query, utilizing HTTP headers instead of semantic URL
curl -H 'Accept: application/jsonl' -H 'Accept-Encoding: gzip' \
    -d "select {{ script: [1,2,3] }} n, '{{script:['a','b']}}' s" \
    http://localhost:8080/query | gzip -d

# shutdown and remove the bridge server
docker stop bridge
```

You can utilize the bridge server in [DuckDB](https://duckdb.org/) and [ClickHouse](https://clickhouse.com/) for seamless data loading and querying across databases.

```sql
-- data loading (requires JDBCX driver)
-- same as: insert into my_table (n, s) VALUES (1, 'a') (2, 'b')
insert into my_table {{ values.db.duckdb-local:
  select 1 n, 'a' s union select 2, 'b' }}
-- same query using bridge extension
insert into my_table {{ bridge(path=query/unique-id.values):
  select 1 n, 'a' s union select 2, 'b' }}

-- on DuckDB
select t1.a, t2.b
from read_parquet('http://<my-bridge-server>/query/<unique-id1>.parquet?q=select%20%271%27%20a&codec=zstd') t1
inner join read_csv_auto('http://<my-bridge-server>/query/<unique-id2>.csv.gz?q=select%20%271%27%20b',
    header=true, compression='gzip') t2 on t1.a=t2.b
-- or below if you're using JDBCX driver
select t1.a, t2.b
from {{ table.db.duckdb-local: select '1' a }} t1
inner join {{ table.db.duckdb-local: select '1' b }} t2 on t1.a=t2.b

-- on ClickHouse
select *
from url('http://<my-bridge-server>/query/<unique-id>.parquet?q=select%20%271%27%20a&codec=zstd', Parquet)
-- or below if you're using JDBCX driver
select * from {{ table.db.ch-play: select '1' a }}
```

## Configuration

### Server

Bridge server uses the same `~/.jdbcx/config.properties` file for configuration. Please refer to [this](/docker/app/.jdbcx/config.properties) used in docker image, and named database connections configured at [here](/docker/app/.jdbcx/db).

| Key                  | Value                                | Remark                                                                    |
| -------------------- | ------------------------------------ | ------------------------------------------------------------------------- |
| jdbcx.server.auth    | Defaults to `false`                  | Whether to enable authentication                                          |
| jdbcx.server.secret  | Defaults to empty string             | Secret key to sign and verify access token when authentication is enabled |
| jdbcx.server.host    | Defaults to `0.0.0.0`                | Listen address of the server                                              |
| jdbcx.server.port    | Defaults to `8080`                   | Listen port of the server                                                 |
| jdbcx.server.context | Defaults to `/`                      | Web context of the server, must be ends with `/`                          |
| jdbcx.server.url     | Defaults to `http://127.0.0.1:8080/` | Bridge server URL that can be accessed remotely, must be ends with `/`    |

### Datasource

`datasource.properties` defines the default database used by bridge server. Please refer to [this](/docker/app/datasource.properties) used in docker image.

### Authentication

To enable authentication, set the property `jdbcx.server.auth` to `true`. It's highly recommended to also configure a secure `jdbcx.server.secret`, which should be 512-bit or longer for security. Authentication is disabled when `jdbcx.server.auth` is set to `false`.

```bash
# generate a secure key
$ openssl rand -base64 66 | tr -d '\n'
QXk18EEgn9r2xf2NF6PXUf9L9kI7srS4L89lXJp9f+iVkp6ugZICjAu5iD4LOdFEyXHcXp/mnJ3K4wu85D77kV1w

$ grep 'jdbcx.server.' config.properties
...
jdbcx.server.auth=true
jdbcx.server.secret=HS512:QXk18EEgn9r2xf2NF6PXUf9L9kI7srS4L89lXJp9f+iVkp6ugZICjAu5iD4LOdFEyXHcXp/mnJ3K4wu85D77kV1w
...

# create a temporary access token for the client, valid for one day (1,440 minutes).
$ docker run --rm -it -v `pwd`/config.properties:/app/.jdbcx/config.properties jdbcx/jdbcx \
  token 'issuer=https://my.company.com;subject=my@email.address;expires=1440;allowed_ips=192.168.1.0/24'
eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJodHRwczovL215LmNvbXBhbnkuY29tIiwic3ViIjoibXlAZW1haWwuYWRkcmVzcyIsImp0aSI6Ijk2ZWNkMjk1LTUxYmMtNGFjMy04MTkzLWYxMDY5MTRkNThiYSIsImlhdCI6MTc1OTk2Mjk1MywiZXhwIjoxNzYwMDQ5MzUzLCJhbGxvd2VkX2lwcyI6IjE5Mi4xNjguMS4wLzI0In0.ME9i19RUME6NGwLfoP_nayqfXLrm6uFsQ-9Ghcv03gyXyGoL86XW-AdUgstaJIUwa6QoU2WydOWM25yo5UOiug
```
