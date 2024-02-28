# JDBCX Driver

JDBCX driver is essentially a wrapper for the JDBC driver, offering features like multi query languages, dynamic query, query substitution, type mapping, and more. When configured with bridge server, it simplifies federated queries.


## Quick Start

:information_source: Before you begin, please create a [config.properties](/docker/app/.jdbcx/config.properties) file (`~/.jdbcx/config.properties` on macOS/Linux, `%HOMEPATH%\.jdbcx\config.properties` on Windows) if you don't already have one.

### DBeaver

Starting from DBeaver 23.2.1, you can create a JDBCX connection just like any other database connection.

1. From the main menu, select `Database`, and then choose `New Database Connection`
    ![image](https://github.com/jdbcx/jdbcx/assets/4270380/75929b8a-709a-4ece-9c25-c760d882cec0)

2. Choose `JDBCX`, download the latest driver if you haven't already, and then click `Finish`
    ![image](https://github.com/jdbcx/jdbcx/assets/4270380/9b624cec-c434-40a8-93b2-a6555359dca8)

3. Optionally, modify driver properties by editing the connection
    ![image](https://github.com/jdbcx/jdbcx/assets/4270380/274e94cc-55bc-47c2-87fd-da313335dd74)

4. Open the connection and execute your query
    ![image](https://github.com/jdbcx/jdbcx/assets/4270380/c6dd84ac-eb83-439b-b6d5-e5e68b46a7db)

### Docker

:information_source: If you're using Windows, please replace the trailing backslash `\` with a caret `^`.

```bash
#
# Mixed
#
# "-DnoProperties=true" is only required for DuckDB, because its JDBC driver does not work with unsupported property
docker run --rm -it -e DRIVER_OPTS="-DnoProperties=true" jdbcx/jdbcx "jdbcx:duckdb:" \
    "select '{{ shell: echo 1 }}' as one, '{{ db.ch-play: select 2 }}' as two, {{ script: 1+2 }} as three"

#
# PRQL
#
docker run --rm -it jdbcx/jdbcx "jdbcx:sqlite::memory:" "{{ prql: from sqlite_schema }}"
# or change the default query language to PRQL
docker run --rm -it jdbcx/jdbcx "jdbcx:prql:sqlite::memory:" "from sqlite_schema"

#
# Scripting
#
docker run --rm -it -e DRIVER_OPTS="-Dverbose=true -DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:duckdb:" "select '{{ script: conn.getClass() }}'"
# or change the default language to shell
docker run --rm -it -e DRIVER_OPTS="-Dverbose=true -DoutputFormat=TSVWithHeaders" jdbcx/jdbcx \
    "jdbcx:script:sqlite::memory:" "helper.format('SELECT 1 %s, 2 %s', 'one', 'two')"


#
# Shell
#
docker run --rm -it -e DRIVER_OPTS="-Dverbose=true -DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:duckdb:" "{{ shell(exec.timeout=1000): echo 123}}"
# or change the default language to shell
docker run --rm -it -e DRIVER_OPTS="-Dverbose=true -DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:shell:duckdb:" "echo select 123"


#
# SQL
#
docker run --rm -it -e DRIVER_OPTS="-DnoProperties=true" jdbcx/jdbcx \
    "jdbcx:duckdb:" "select '{{ db.ch-altinity: select 1 }}' as one, '{{ db.ch-play: select 2 }}' as two"
```

### Command Line

```bash
# Download latest JDBCX driver from https://github.com/jdbcx/jdbcx/releases
# or use nightly build at https://s01.oss.sonatype.org/content/repositories/snapshots/io/github/jdbcx/jdbcx-driver/
wget -O jdbcx.jar $(curl -sL https://api.github.com/repos/jdbcx/jdbcx/releases/latest \
        | grep "browser_download_url.*jdbcx-driver.*.jar" | tail -1 \
        | cut -d : -f 2,3 | tr -d \")

# Try shell command
java -jar jdbcx.jar 'jdbcx:' '{{ shell: echo Yes }}'

# Download SQLite JDBC driver and its dependency
wget https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.45.1.0/sqlite-jdbc-3.45.1.0.jar \
    https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.12/slf4j-api-2.0.12.jar \
    https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.12/slf4j-simple-2.0.12.jar

# Download JavaScript engine
wget https://repo1.maven.org/maven2/org/mozilla/rhino/1.7.14/rhino-1.7.14.jar \
    https://repo1.maven.org/maven2/org/mozilla/rhino-engine/1.7.14/rhino-engine-1.7.14.jar

# SQL
java -Djdbcx.custom.classpath=. -jar jdbcx.jar 'jdbcx:sqlite::memory:' 'select 1'

# Scripting
java -Djdbcx.custom.classpath=. -jar jdbcx.jar \
    'jdbcx:script:sqlite::memory:' 'helper.format("SELECT %s, %s", "1", "2")'

# PRQL
wget -O prqlc.deb https://github.com/PRQL/prql/releases/download/0.11.4/prqlc_0.11.4_$(arch | sed -e 's|aarch64|arm64|' -e 's|x86_64|amd64|').deb
dpkg -i prqlc.deb
java -Djdbcx.custom.classpath=. -Djdbcx.prql.cli.path=/usr/bin/prqlc -jar jdbcx.jar \
    'jdbcx:prql:sqlite::memory:' 'from `sqlite_schema`'

# Together on a database in cloud
wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-http.jar
cat << 'EOF' > my.js
helper.format(
	"select '%s' conn, arrayJoin(splitByChar('\\n', '%s')) result",
	conn, // current java.sql.Connection instance
	helper.escapeSingleQuote(helper.cli("prqlc", "-h"))
)
EOF
java -Djdbcx.custom.classpath=`pwd` -jar jdbcx.jar \
    'jdbcx:script:ch://explorer@play.clickhouse.com:443?ssl=true&compress=0' @my.js
```


## Configuration

### config.properties

By default, JDBCX loads its configuration from [config.properties](/docker/app/.jdbcx/config.properties) file. It's highly recommended to create this file for simplicity and to share the same configuration across database connections on the same host.


- `~/.jdbcx/config.properties` on macOS/Linux

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

- `%HOMEPATH%\.jdbcx\config.properties` on Windows

  ```properties
  jdbcx.custom.classpath=D:/My/Jdbc/Drivers

  # If you prefer WSL
  #jdbcx.prql.cli.path=wsl -- /home/<user>/.cargo/bin/prqlc
  #jdbcx.shell.cli.path=wsl -- /bin/bash -c
  ```

| Key | Value | Remark |
| --- | ----- | ------ |
| jdbcx.custom.classpath | | Path to JDBC drivers and any other required jar files(e.g. those for scripting) |
| jdbcx.server.url |  | Bridge server URL that ends with `/`, for example: `http://192.168.3.100:8080/` |
| jdbcx.server.token |  | Token required for accessing the bridge server if authentication is enabled |
| jdbcx.tag | Defaults to `BRACE` | Change variable tag to use angle/square bracket |

### Named Database

For security reasons, it's often better to access a database using a simplified name, such as `{{ db.mysql1: select 1 }}`, rather than specifying the full connection URL like `{{ db(url=jdbc:mysql://localhost:3306): select 1 }}`. You can declare a database connection by creating a property file under `~/.jdbcx/db` on macOS/Linux (or `%HOMEPATH%\.jdbcx\db` on Windows).

```properties
# ~/.jdbcx/db/mysql1.properties on macOS/Linux
# %HOMEPATH%\.jdbcx\db\mysql1.properties on Windows
jdbcx.classpath=/path/to/mysql-jdbc.jar
jdbcx.driver=com.mysql.cj.jdbc.Driver
jdbcx.url=jdbc:mysql://192.168.3.233:3306/test
user=test
password=test
```

| Key | Value | Remark |
| --- | ----- | ------ |
| jdbcx.classpath | | JDBC driver classpath, for example: `/path/to/mysql-jdbc.jar` |
| jdbcx.driver | | JDBC driver class name, for example: `com.mysql.cj.jdbc.Driver` |
| jdbcx.url | | JDBC connection URL, for example: `jdbc:mysql://192.168.3.233:3306/test` |
| user | | Username for accessing the database |
| password | | Password required for accessing the database |

Please refer to [these](/docker/app/.jdbcx/db) used in docker image for more information.


## Query Syntax

Your existing queries should continue to work as before, but with JDBCX, you can do much more. Depending on the `jdbcx.tag` setting, you can use curly braces (the default), angle brackets, or square brackets as variable tags to write dynamic queries that may or may not use multiple query languages.

A dynamic query may contain two types of executable blocks:
1. An executable block wrapped between `{{` and `}}` that returns value.
2. An executable block wrapped between `{%` and `%}` that does not return a value.


## Extension

There are many extensions each created for connecting to a different type of datasource. Here below is a quick summary, for detailed usage, you may issue query `{{ help }}` or `{{ <extension> }}` to see more information.
