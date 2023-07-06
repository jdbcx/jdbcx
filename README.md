# JDBCX
JDBCX enhances the JDBC driver by supporting additional data formats, compression algorithms, object mapping, type conversion, and query languages beyond SQL. It simplifies complex federated queries with dynamic query embedding and remote bridge server connectivity for multiple data sources.

## Quick Start

```bash
# Download a tiny embedded database and its JDBC driver
wget https://repo1.maven.org/maven2/org/hsqldb/hsqldb/2.7.2/hsqldb-2.7.2.jar

# Download JavaScript engine
wget https://repo1.maven.org/maven2/org/mozilla/rhino/1.7.14/rhino-1.7.14.jar \
    https://repo1.maven.org/maven2/org/mozilla/rhino-engine/1.7.14/rhino-engine-1.7.14.jar

# SQL
java -Dverbose=true -jar jdbcx-driver-0.1.0.jar 'jdbcx:hsqldb:mem:test' \
    'SELECT * FROM INFORMATION_SCHEMA.ROUTINES'

# Scripting
java -Dverbose=true -jar jdbcx-driver-0.1.0.jar 'jdbcx:script:hsqldb:mem:test' \
    'helper.format("SELECT * FROM %s.%s", "INFORMATION_SCHEMA", "ROUTINES")'

# PRQL
cargo install prqlc
java -Djdbcx.prql.cli.path=~/.cargo/bin/prqlc -Dverbose=true -jar jdbcx-driver-0.1.0.jar \
    'jdbcx:prql:hsqldb:mem:test' 'from `INFORMATION_SCHEMA.ROUTINES`'

# Together on a database in cloud
wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-http.jar
cat << 'EOF' > my.js
helper.format(
	"select '%s' conn, arrayJoin(splitByChar('\\n', '%s')) result",
	conn, // current java.sql.Connection instance
	helper.escapeSingleQuote(helper.cli("~/.cargo/bin/prqlc", "-h"))
)
EOF
java -Djdbcx.custom.classpath=`pwd` -Dverbose=true -jar jdbcx-driver-0.1.0.jar \
    'jdbcx:script:ch://explorer@play.clickhouse.com:443?ssl=true' @my.js
```

## IDE Setup

### DBeaver

Due to [dbeaver/dbeaver#19165](https://github.com/dbeaver/dbeaver/issues/19165), you have to edit existing driver settings by adding `jdbcx-driver` jar file. Alternatively, you may follow instructions below to setup a new database driver.

1. `Database` -> `Driver Manager` -> `New` or `Edit`
    ![image](https://user-images.githubusercontent.com/4270380/251389086-e42d2828-cc68-4306-8595-d300ed1527af.png)
    * **Driver:** `io.github.jdbcx.WrappedDriver`
    * **URL Template:** `jdbcx:[ext:]xxx://{host}[:{port}][/{database}]`

2. `Database` -> `New Database Connection` -> select `JDBCX` and click `Next` -> specify `URL` and switch to `Driver Properties` tab to make changes as needed
    ![image](https://user-images.githubusercontent.com/4270380/251389733-52d8318c-f00a-4f37-8635-72388c91130d.png)

