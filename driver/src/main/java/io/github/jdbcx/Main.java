/*
 * Copyright 2022-2023, Zhichun Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.jdbcx;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Objects;

public final class Main {
    private static void println() {
        System.out.println(); // NOSONAR
    }

    private static void println(Object msg, Object... args) {
        if (args == null || args.length == 0) {
            System.out.println(msg); // NOSONAR
        } else {
            System.out.println(String.format(Locale.ROOT, Objects.toString(msg), args)); // NOSONAR
        }
    }

    private static void printUsage() {
        String execFile = "jdbcx-bin";
        try {
            File file = Paths.get(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .toFile();
            if (file.isFile()) {
                execFile = file.getName();
                if (!Files.isExecutable(file.toPath())) {
                    execFile = "java -jar " + execFile;
                }
            } else {
                execFile = "java -cp " + file.getCanonicalPath() + " " + Main.class.getName();
            }
        } catch (Exception e) {
            // ignore
        }

        final int index = execFile.indexOf(' ');
        println("Usage: %s <JDBC URL> [@FILE or QUERY]",
                index > 0 ? (execFile.substring(0, index) + " [PROPERTIES]" + execFile.substring(index))
                        : (execFile + " [PROPERTIES]"));
        println();
        println("Properties: -Dkey=value [-Dkey=value]*");
        println("  loopCount\tNumber of times to repeat the same query, defaults to 1");
        println("  loopInterval\tInterval in milliseconds between repeated executions, defaults to 0");
        println("  verbose\tWhether to show logs, defaults to false");
        println();
        println("Examples:");
        println("  -  %s 'jdbcx:derby:memory:x;create=True' 'select * from SYS.SYSTABLES'",
                index > 0 ? (execFile.substring(0, index) + " -Dverbose=true" + execFile.substring(index))
                        : (execFile + " -Dverbose=true"));
        println("  -  %s 'jdbcx:script:ch://explorer@play.clickhouse.com:443?ssl=true' @my.js", execFile);
    }

    static long execute(String url, String fileOrQuery) throws IOException, SQLException {
        if (fileOrQuery == null || fileOrQuery.isEmpty()) {
            return 0L;
        } else if (fileOrQuery.charAt(0) == '@') {
            fileOrQuery = Utils.readAllAsString(new FileInputStream(Utils.normalizePath(fileOrQuery.substring(1))));
        }

        try (Connection conn = DriverManager.getConnection(url, System.getProperties());
                Statement stmt = conn.createStatement()) {
            long rows = 0L;
            if (stmt.execute(fileOrQuery)) {
                // only check the first result set
                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next()) {
                        rows++;
                    }
                }
            } else {
                try {
                    rows = stmt.getLargeUpdateCount();
                } catch (Exception e) {
                    rows = stmt.getUpdateCount();
                }
            }
            return rows;
        }
    }

    public static void main(String[] args) throws Exception {
        if ((args == null || args.length < 1) || args.length > 2) {
            printUsage();
            System.exit(0);
        }

        final long loopCount = Long.getLong("loopCount", 1L);
        final long loopInterval = Long.getLong("loopInterval", 0L);
        final boolean verbose = Boolean.parseBoolean(System.getProperty("verbose", Boolean.FALSE.toString()));
        final String url = args[0];
        final String fileOrQuery = args[1];

        long count = 0L;
        boolean failed = false;
        do {
            final long startTime = verbose ? System.nanoTime() : 0L;
            final long rows = execute(url, fileOrQuery);
            if (verbose) {
                long elapsedNanos = System.nanoTime() - startTime;
                println("\nProcessed %,d rows in %,.2f ms (%,.2f rows/s)", rows, elapsedNanos / 1_000_000D,
                        rows * 1_000_000_000D / elapsedNanos);
            }
            failed = failed || rows < 1L;
            count++;
            if (loopInterval > 0L) {
                if (verbose) {
                    println("Sleep for %,d ms...", loopInterval);
                }
                Thread.sleep(loopInterval);
            }
        } while (count < loopCount);
        System.exit(failed ? 1 : 0);
    }

    private Main() {
    }
}
