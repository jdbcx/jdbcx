# JDBCX

[![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/jdbcx/jdbcx?style=plastic&include_prereleases&label=Latest%20Release)](https://github.com/jdbcx/jdbcx/releases/) [![GitHub release (by tag)](https://img.shields.io/github/downloads/jdbcx/jdbcx/latest/total?style=plastic)](https://github.com/jdbcx/jdbcx/releases/) [![Docker Pulls](https://img.shields.io/docker/pulls/jdbcx/jdbcx?style=plastic)](https://hub.docker.com/r/jdbcx/jdbcx) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=jdbcx_jdbcx&metric=coverage)](https://sonarcloud.io/summary/new_code?id=jdbcx_jdbcx) [![Sonatype Nexus (Snapshots)](https://img.shields.io/nexus/s/io.github.jdbcx/jdbcx?style=plastic&label=Nightly%20Build&server=https%3A%2F%2Fs01.oss.sonatype.org)](https://s01.oss.sonatype.org/content/repositories/snapshots/io/github/jdbcx/)

<img align="right" width="96" height="96" src="https://avatars.githubusercontent.com/u/137983508">

JDBCX enhances the JDBC driver by supporting additional data formats, compression algorithms, object mapping, type conversion, and query languages beyond SQL. It simplifies complex federated queries with dynamic query embedding and remote bridge server connectivity for multiple data sources.

## Quick Start

Getting started with JDBCX is a breeze. Just download it from [GitHub Releases](https://github.com/jdbcx/jdbcx/releases/) or [Maven Central](https://repo1.maven.org/maven2/io/github/jdbcx/jdbcx-driver/), add it to your classpath, and modify your JDBC connection string by replacing `jdbc:` with `jdbcx:`. For PRQL over SQL, simply switch to `jdbcx:prql:` to activate the extension.

### Docker

```bash
# SQL
docker run --rm -it -e VERBOSE=true jdbcx/jdbcx \
    'jdbcx:derby:memory:x;create=true' 'select * from SYS.SYSTABLES'

# Scripting
docker run --rm -it -e VERBOSE=true jdbcx/jdbcx \
    'jdbcx:script:derby:memory:x;create=true' \
    'helper.format("SELECT * FROM %s.%s", "SYS", "SYSTABLES")'

# PRQL
docker run --rm -it -e VERBOSE=true jdbcx/jdbcx \
    'jdbcx:prql:derby:memory:x;create=true' 'from `SYS.SYSTABLES`'

# Shell
docker run --rm -it -e VERBOSE=true jdbcx/jdbcx \
    'jdbcx:shell:derby:memory:x;create=true' 'echo "select * from SYS.SYSTABLES"'
```

### Command Line

```bash
# Download latest JDBCX from https://github.com/jdbcx/jdbcx/releases
# or use nightly build at https://s01.oss.sonatype.org/content/repositories/snapshots/io/github/jdbcx/jdbcx-driver/
wget -O jdbcx.jar $(curl -sL https://api.github.com/repos/jdbcx/jdbcx/releases/latest \
        | grep "browser_download_url.*jdbcx-driver.*.jar" | tail -1 \
        | cut -d : -f 2,3 | tr -d \")

# Download Apache Derby embedded database and its JDBC driver
wget https://repo1.maven.org/maven2/org/apache/derby/derby/10.16.1.1/derby-10.16.1.1.jar \
    https://repo1.maven.org/maven2/org/apache/derby/derbyshared/10.16.1.1/derbyshared-10.16.1.1.jar

# Download JavaScript engine
wget https://repo1.maven.org/maven2/org/mozilla/rhino/1.7.14/rhino-1.7.14.jar \
    https://repo1.maven.org/maven2/org/mozilla/rhino-engine/1.7.14/rhino-engine-1.7.14.jar

# SQL
java -Dverbose=true -jar jdbcx.jar 'jdbcx:derby:memory:x;create=True' 'select * from SYS.SYSTABLES'

# Scripting
java -Dverbose=true -jar jdbcx.jar 'jdbcx:script:derby:memory:x;create=True' \
    'helper.format("SELECT * FROM %s.%s", "SYS", "SYSTABLES")'

# PRQL
cargo install prqlc
java -Djdbcx.prql.cli.path=~/.cargo/bin/prqlc -Dverbose=true -jar jdbcx.jar \
    'jdbcx:prql:derby:memory:x;create=True' 'from `SYS.SYSTABLES`'

# Together on a database in cloud
wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-http.jar
cat << 'EOF' > my.js
helper.format(
	"select '%s' conn, arrayJoin(splitByChar('\\n', '%s')) result",
	conn, // current java.sql.Connection instance
	helper.escapeSingleQuote(helper.cli("~/.cargo/bin/prqlc", "-h"))
)
EOF
java -Djdbcx.custom.classpath=`pwd` -Dverbose=true -jar jdbcx.jar \
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

## Performance

### Test Environment
* JDK: openjdk version "17.0.7" 2023-04-18
* Tool: Apache JMeter 5.6.2
* Database: ClickHouse 22.8
* JDBC Driver: clickhouse-jdbc v0.4.6

### Test Configuration
* Concurrent Users: 20
* Loop Count: 1000
* Connection Pool:
  - Size: 30
  - Init SQL and Validation Query are identical

### Test Results

| Connection | Init SQL | Test Query | Avg Response Time (ms) | Max Response Time (ms) | Throughput (qps) |
| ---------- | -------- | ---------- | ---------------------- | ---------------------- | ---------------- |
| `jdbc:ch` | select * from system.numbers limit 1 | select * from system.numbers limit 50000 | 69 | 815 | 279.87 |
| `jdbcx:ch` | select * from system.numbers limit 1 | select * from system.numbers limit 50000 | 71 | 891 | 272.99 |
| `jdbcx:script:ch` | 'select * from system.numbers limit 1' | 'select * from system.numbers limit ' + 50000 | 72 | 1251 | 270.65 |
| `jdbcx:shell:ch` | echo 'select * from system.numbers limit 1' | echo 'select * from system.numbers limit 50000' | 91 | 650 | 214.45 |
| `jdbcx:prql:ch` | from \`system.numbers\` \| take 1 | from \`system.numbers\` \| take 50000 | 106 | 1103 | 184.27 |
