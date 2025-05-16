# JDBCX

[![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/jdbcx/jdbcx?style=plastic&include_prereleases&label=Latest%20Release)](https://github.com/jdbcx/jdbcx/releases/) [![GitHub release (by tag)](https://img.shields.io/github/downloads/jdbcx/jdbcx/latest/total?style=plastic)](https://github.com/jdbcx/jdbcx/releases/) [![Docker Pulls](https://img.shields.io/docker/pulls/jdbcx/jdbcx?style=plastic)](https://hub.docker.com/r/jdbcx/jdbcx) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=jdbcx_jdbcx&metric=coverage)](https://sonarcloud.io/summary/new_code?id=jdbcx_jdbcx) [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/io.github.jdbcx/jdbcx?style=plastic&label=Nightly%20Build&server=https%3A%2F%2Fs01.oss.sonatype.org)](https://s01.oss.sonatype.org/content/repositories/snapshots/io/github/jdbcx/)

<img align="right" width="96" height="96" src="https://avatars.githubusercontent.com/u/137983508">

JDBCX extends JDBC with enhanced data format and compression support, object mapping, advanced type conversion, and multi-language query capabilities. It simplifies federated queries through dynamic embedding and offers a remote bridge for seamless multi-source connectivity.

![image](https://user-images.githubusercontent.com/4270380/257034477-a5e1fe1a-bb1c-4478-addc-43fbdf4e4d07.png)

## Quick Start

Getting started with JDBCX is easy. Use it as a standard [JDBC driver](/driver), a standalone [bridge server](/server), or combine both functionalities.

```bash
# Using the standard JDBC driver
$ docker run --rm -it jdbcx/jdbcx 'jdbc:duckdb:' 'select 2 as num'
num
2

# Using JDBCX as a drop-in replacement (with extensions)
$ docker run --rm -it jdbcx/jdbcx 'jdbcx:duckdb:' 'select {{script:1+1}} as num'
num
2
$ docker run --rm -it jdbcx/jdbcx 'jdbcx:' '{{ db.ch-[ap]*: select ''${_.id}'' db, version() ver }}'
db	ver
ch-play	25.5.1.664
ch-altinity	25.3.3.42

# Using JDBCX as a bridge server (HTTP API for data access)
$ docker run --rm -d --name jdbcx-bridge -p8080:8080 jdbcx/jdbcx server
$ curl -s -d 'select 2 as num' 'http://localhost:8080/query'
num
2
$ curl -s -d '{{ db.duckdb-local: select 2 as num }}' 'http://localhost:8080/query'
num
2

# Combining JDBCX driver features with the bridge server for federated querying
$ curl -s -d 'select * from {{ table.db.duckdb-local: select 2 as num }}' 'http://localhost:8080/query'
num
2
```

## Features

<table>
<tr>
<td> Feature </td> <td> Examples </td>
</tr>
<tr>
<td> Chained query </td>
<td>

```sql
-- ask a question(check out ~/.jdbcx/web/baidu-*.properties for details)
{{ web.baidu-llm(pre.query=web.baidu-auth): who are you? }}

-- get messages of a chat(see ~/.jdbcx/web/m365-*.properties for details)
{{ web.m365-graph(
	pre.query=web.m365-auth,
	result.json.path=value,
	m365.api="chats/<URL encoded chat ID>/messages?$top=50")
}}
```

</td>
</tr>
<tr>
<td> Dynamic query </td>
<td>

```sql
-- https://clickhouse.com/docs/en/sql-reference/aggregate-functions/parametric-functions#retention
{% var(delimiter=;): dates=['2020-01-01','2020-01-02','2020-01-03'] %}
SELECT
    uid,
    retention({{ script: "date='" + ${dates}.join("',date='") + "'" }}) AS r
FROM retention_test
WHERE date IN ({{ script: "'" + ${dates}.join("','") + "'" }})
GROUP BY uid
ORDER BY uid ASC
```

</td>
</tr>
<tr>
<td> Multi-language query </td>
<td>

```sql
{% var: num=3 %}
select {{ script: ${num} - 2 }} one,
    {{ shell: echo 2 }} two,
    {{ db.ch-play: select ${num} }} three
```

</td>
</tr>
<tr>
<td> Query substitution </td>
<td>

```sql
{% var: func=toYear, sdate='2023-01-01' %}
SELECT ${func}(create_date) AS d, count(1) AS c
FROM my_table
WHERE create_date >= ${sdate}
GROUP BY d
```

</td>
</tr>
<tr>
<td> Scripting </td>
<td>

```sql
-- benchmark on ClickHouse
select a[1] `CPU%`, a[2] `MEM(KB)`, a[3] `Elapsed Time(s)`,
	a[4] `CPU Time(s)`, a[5] `User Time(s)`, a[6] `Switches`,
	a[7] `Waits`, a[8] `File Inputs`, a[9] `File Outputs`, a[10] `Swaps`
from (
select splitByChar(',', '{{ shell.myserver(cli.stderr.redirect=true):
/bin/time -f '%P,%M,%e,%S,%U,%c,%w,%I,%O,%W' du -sh . > /dev/null
}}') a
)

-- runtime inspection
{{ script: helper.table(
  // fields
  ['connection_class_loader', 'current_class_loader', 'context_class_loader'],
  // rows
  [
    [
      Packages.io.github.jdbcx.WrappedDriver.__javaObject__.getClassLoader(),
      helper.getClass().getClassLoader(),
      java.lang.Thread.currentThread().getContextClassLoader()
    ]
  ]
)
}}
```

</td>
</tr>
</table>

## Known Issues

| #   | Issue                                     | Workaround                          |
| --- | ----------------------------------------- | ----------------------------------- |
| 1   | Query cancellation is not fully supported | avoid query like `{{ shell: top }}` |
| 2   | Connection pooling is not supported       | -                                   |
| 3   | Nested query is not supported             | -                                   |
| 4   | MCP extension requires JDK 17+            | -                                   |

## Performance

### Test Environment

- JDK: openjdk version "17.0.7" 2023-04-18
- Tool: Apache JMeter 5.6.2
- Database: ClickHouse 22.8
- JDBC Driver: clickhouse-jdbc v0.4.6

### Test Configuration

- Concurrent Users: 20
- Loop Count: 1000
- Connection Pool:
  - Size: 30
  - Init SQL and Validation Query are identical

### Test Results

| Connection        | Init SQL                                     | Test Query                                       | Avg Response Time (ms) | Max Response Time (ms) | Throughput (qps) |
| ----------------- | -------------------------------------------- | ------------------------------------------------ | ---------------------- | ---------------------- | ---------------- |
| `jdbc:ch`         | select \* from system.numbers limit 1        | select \* from system.numbers limit 50000        | 69                     | 815                    | 279.87           |
| `jdbcx:ch`        | select \* from system.numbers limit 1        | select \* from system.numbers limit 50000        | 71                     | 891                    | 272.99           |
| `jdbcx:script:ch` | 'select \* from system.numbers limit 1'      | 'select \* from system.numbers limit ' + 50000   | 72                     | 1251                   | 270.65           |
| `jdbcx:shell:ch`  | echo 'select \* from system.numbers limit 1' | echo 'select \* from system.numbers limit 50000' | 91                     | 650                    | 214.45           |
| `jdbcx:prql:ch`   | from \`system.numbers\` \| take 1            | from \`system.numbers\` \| take 50000            | 106                    | 1103                   | 184.27           |
