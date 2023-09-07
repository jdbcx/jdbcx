# JDBCX

[![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/jdbcx/jdbcx?style=plastic&include_prereleases&label=Latest%20Release)](https://github.com/jdbcx/jdbcx/releases/) [![GitHub release (by tag)](https://img.shields.io/github/downloads/jdbcx/jdbcx/latest/total?style=plastic)](https://github.com/jdbcx/jdbcx/releases/) [![Docker Pulls](https://img.shields.io/docker/pulls/jdbcx/jdbcx?style=plastic)](https://hub.docker.com/r/jdbcx/jdbcx) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=jdbcx_jdbcx&metric=coverage)](https://sonarcloud.io/summary/new_code?id=jdbcx_jdbcx) [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/io.github.jdbcx/jdbcx?style=plastic&label=Nightly%20Build&server=https%3A%2F%2Fs01.oss.sonatype.org)](https://s01.oss.sonatype.org/content/repositories/snapshots/io/github/jdbcx/)

<img align="right" width="96" height="96" src="https://avatars.githubusercontent.com/u/137983508">

JDBCX enhances the JDBC driver by supporting additional data formats, compression algorithms, object mapping, type conversion, and query languages beyond SQL. It simplifies complex federated queries with dynamic query embedding and remote bridge server connectivity for multiple data sources.

![image](https://user-images.githubusercontent.com/4270380/257034477-a5e1fe1a-bb1c-4478-addc-43fbdf4e4d07.png)


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

-- get messages of a chat(see ~/.jdbcx/web/ms365-*.properties for details)
{{ web.ms365-graph(
	pre.query=web.ms365-auth,
	result.json.path=value,
	ms365.api="chats/<URL encoded chat ID>/messages?$top=50")
}}
```

</td>
</tr>
<tr>
<td> Dynamic query </td>
<td>

```sql
-- https://clickhouse.com/docs/en/sql-reference/aggregate-functions/parametric-functions#retention
SELECT
    uid,
    retention({{ script: "date='" + ['2020-01-01','2020-01-02','2020-01-03'].join("',date='") + "'" }}) AS r
FROM retention_test
WHERE date IN ({{ script: "'" + ['2020-01-03','2020-01-02','2020-01-01'].join("','") + "'" }})
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
{% var: func=toYear, sdate=2023-01-01 %}
SELECT ${func}(create_date) AS d, count(1) AS c
FROM my_table
WHERE create_date >= '${sdate}'
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


## Quick Start

Getting started with JDBCX is easy.

![image](https://user-images.githubusercontent.com/4270380/263505625-f62b10f1-e3e9-4037-9ab0-2925191f851e.png)

1. Download the JDBCX driver JAR (e.g. `jdbcx-driver-<version>.jar`) from one of these sources:
   * GitHub Releases: https://github.com/jdbcx/jdbcx/releases/
   * Maven Central: https://repo1.maven.org/maven2/io/github/jdbcx/jdbcx-driver/
2. Add the JAR to your classpath
3. Create a new connection to `jdbcx:`, or update your existing JDBC connection string by replacing the `jdbc:` prefix with `jdbcx:`, for instance:
   ```
   // Update existing JDBC connection
   jdbc:duckdb: 
   -->
   jdbcx:duckdb:
   ```
4. To use PRQL instead of SQL, change the prefix to `jdbcx:prql:`, for example:
   ```
   // Update existing JDBC connection
   jdbc:duckdb: 
   -->
   jdbcx:prql:duckdb:
   ```


## Configuration

It is recommended to create a [property file](https://en.wikipedia.org/wiki/.properties) in the following locations:

- ~/.jdbcx/config.properties on macOS/Linux

  ```properties
  # a directory contains all required JDBC drivers, and dependencies if any
  jdbcx.custom.classpath=/path/to/jdbc/drivers

  # CodeQL example
  #jdbcx.codeql.cli.path=/opt/homebrew/bin/codeql
  #jdbcx.codeql.database=~/Sources/Local/CodeQL/jdbcx
  #jdbcx.codeql.work.dir=~/Sources/Github/jdbcx

  # cargo install prqlc
  jdbcx.prql.cli.path=~/.cargo/bin/prqlc
  ```

- %HOMEPATH%\\.jdbcx\config.properties on Windows

  ```properties
  jdbcx.custom.classpath=D:/My/Jdbc/Drivers

  # If you prefer WSL
  #jdbcx.prql.cli.path=wsl -- /home/<user>/.cargo/bin/prqlc
  #jdbcx.shell.cli.path=wsl -- /bin/bash -c
  ```

JDBCX also supports simplified connection strings like `jdbcx:db:<name>` for predefined connections.

To use this:

1. Create a properties file under `~/.jdbcx/db` to define your connection.
    For example:
    ```properties
    # ~/.jdbcx/db/my-connection.properties on macOS/Linux
    # %HOMEPATH%\.jdbcx\db\my-connection.properties on Windows
    jdbcx.classpath=/path/to/duckdb/jdbc/driver
    jdbcx.driver=org.duckdb.DuckDBDriver
    jdbcx.url=jdbc:duckdb:
    ```
2. Reference the connection in your code `jdbcx:db:my-connection`
3. You can also reference named connections directly in your queries:
    ```sql
    {{ db(id=my-connection): SELECT * FROM table }}

    -- or dot notation
    {{ db.my-connection: SELECT * FROM table }}
    ```

See the [examples](https://github.com/jdbcx/jdbcx/tree/main/docker/app/.jdbcx/) for more details.


## Usage

### Docker

:information_source: If you're using Windows, please replace the trailing backslash `\` with a caret `^`.

```bash
#
# Mixed
#
# "-DnoProperties=true" is only required for DuckDB, because its JDBC driver does not work with unsupported property
docker run --rm -it -e JDBCX_OPTS="-DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:duckdb:" "select '{{ shell: echo 1 }}' as one, '{{ db(id=ch-play): select 2 }}' as two, {{ script: 1+2 }} as three"


#
# PRQL
#
docker run --rm -it jdbcx/jdbcx \
    "jdbcx:derby:memory:x;create=true" "{{ prql: from SYS.SYSTABLES }}"
# or change the default query language to PRQL
docker run --rm -it jdbcx/jdbcx \
    "jdbcx:prql:derby:memory:x;create=true" "from SYS.SYSTABLES"


#
# Scripting
#
docker run --rm -it -e JDBCX_OPTS="-Dverbose=true -DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:duckdb:" "select '{{ script: conn.getClass()}}'"
# or change the default language to shell
docker run --rm -it -e JDBCX_OPTS="-Dverbose=true -DoutputFormat=TSVWithHeaders" jdbcx/jdbcx \
    "jdbcx:script:derby:memory:x;create=true" "helper.format('SELECT * FROM %s.%s', 'SYS', 'SYSTABLES')"


#
# Shell
#
docker run --rm -it -e JDBCX_OPTS="-Dverbose=true -DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:duckdb:" "{{ shell(exec.timeout=1000): echo 123}}"
# or change the default language to shell
docker run --rm -it -e JDBCX_OPTS="-Dverbose=true -DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:shell:duckdb:" "echo select 123"


#
# SQL
#
docker run --rm -it -e JDBCX_OPTS="-DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:duckdb:" "select '{{ db(id=ch-altinity,exec.timeout=0): select 1 }}' as one, '{{ db(id=ch-play): select 2 }}' as two"
```

### Command Line

```bash
# Download latest JDBCX from https://github.com/jdbcx/jdbcx/releases
# or use nightly build at https://s01.oss.sonatype.org/content/repositories/snapshots/io/github/jdbcx/jdbcx-driver/
wget -O jdbcx.jar $(curl -sL https://api.github.com/repos/jdbcx/jdbcx/releases/latest \
        | grep "browser_download_url.*jdbcx-driver.*.jar" | tail -1 \
        | cut -d : -f 2,3 | tr -d \")

# Try direct connect
java -jar jdbcx.jar 'jdbcx:shell' 'echo Yes'

# Download Apache Derby embedded database and its JDBC driver
wget https://repo1.maven.org/maven2/org/apache/derby/derby/10.14.2.0/derby-10.14.2.0.jar

# Download JavaScript engine
wget https://repo1.maven.org/maven2/org/mozilla/rhino/1.7.14/rhino-1.7.14.jar \
    https://repo1.maven.org/maven2/org/mozilla/rhino-engine/1.7.14/rhino-engine-1.7.14.jar

# SQL
java -Djdbcx.custom.classpath=. -jar jdbcx.jar \
    'jdbcx:derby:memory:x;create=true' 'select * from SYS.SYSTABLES'

# Scripting
java -Djdbcx.custom.classpath=. -jar jdbcx.jar \
    'jdbcx:script:derby:memory:x;create=true' 'helper.format("SELECT * FROM %s.%s", "SYS", "SYSTABLES")'

# PRQL
cargo install prqlc
java -Djdbcx.custom.classpath=. -Djdbcx.prql.cli.path=~/.cargo/bin/prqlc -jar jdbcx.jar \
    'jdbcx:prql:derby:memory:x;create=true' 'from `SYS.SYSTABLES`'

# Together on a database in cloud
wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-http.jar
cat << 'EOF' > my.js
helper.format(
	"select '%s' conn, arrayJoin(splitByChar('\\n', '%s')) result",
	conn, // current java.sql.Connection instance
	helper.escapeSingleQuote(helper.cli("~/.cargo/bin/prqlc", "-h"))
)
EOF
java -Djdbcx.custom.classpath=`pwd` -jar jdbcx.jar \
    'jdbcx:script:ch://explorer@play.clickhouse.com:443?ssl=true' @my.js
```

### DBeaver

Due to [dbeaver/dbeaver#19165](https://github.com/dbeaver/dbeaver/issues/19165), you have to edit existing driver settings by adding `jdbcx-driver` jar file. Alternatively, you may follow instructions below to setup a new database driver.

1. `Database` -> `Driver Manager` -> `New` or `Edit`
   ![image](https://user-images.githubusercontent.com/4270380/251389086-e42d2828-cc68-4306-8595-d300ed1527af.png)

   - **Driver:** `io.github.jdbcx.WrappedDriver`
   - **URL Template:** `jdbcx:[ext:]xxx://{host}[:{port}][/{database}]`

2. `Database` -> `New Database Connection` -> select `JDBCX` and click `Next` -> specify `URL` and switch to `Driver Properties` tab to make changes as needed
   ![image](https://user-images.githubusercontent.com/4270380/251389733-52d8318c-f00a-4f37-8635-72388c91130d.png)

## Known Issues

| #   | Issue                                     | Workaround                          |
| --- | ----------------------------------------- | ----------------------------------- |
| 1   | Query cancellation is not fully supported | avoid query like `{{ shell: top }}` |
| 2   | Scripting does not work on DBeaver        | use JDK instead of JRE              |
| 3   | Connection pooling is not supported       | -                                   |
| 4   | Multi-ResultSet is not fully supported    | -                                   |
| 5   | Nested query is not supported             | -                                   |

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
