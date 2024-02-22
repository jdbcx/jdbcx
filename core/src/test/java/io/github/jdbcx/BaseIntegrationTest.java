/*
 * Copyright 2022-2024, Zhichun Wu
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.github.jdbcx.executor.Stream;

/**
 * Adaptive integration test environment.
 */
@SuppressWarnings("squid:S2187")
public class BaseIntegrationTest {
    private static final DockerComposeContainer<?> containers;

    private static final Option CLICKHOUSE_SERVER = Option
            .of(new String[] { "clickhouse.server", "ClickHouse server address", "" });
    private static final String CLICKHOUSE_SERVICE = "clickhouse";
    private static final int CLICKHOUSE_PORT = 8123;

    private static final Option POSTGRESQL_SERVER = Option
            .of(new String[] { "postgresql.server", "PostgreSQL server address", "" });
    private static final String POSTGRESQL_SERVICE = "postgresql";
    private static final int POSTGRESQL_PORT = 5432;

    private static final Option PROXY_SERVER = Option.of(new String[] { "proxy.server", "Proxy server address", "" });
    private static final String PROXY_SERVICE = "toxiproxy";
    private static final int PROXY_PORT = 8474; // control port

    private static final String clickhouseServer;
    private static final String postgresqlServer;
    private static final String proxyServer;

    private static final String serverUrl;

    static {
        Properties props = new Properties();
        try (InputStream in = Utils.getFileInputStream("test.properties")) {
            props.load(in);
        } catch (Exception e) {
            // ignore
        }

        clickhouseServer = CLICKHOUSE_SERVER.getValue(props,
                CLICKHOUSE_SERVER.getEffectiveDefaultValue(Constants.EMPTY_STRING));
        postgresqlServer = POSTGRESQL_SERVER.getValue(props,
                POSTGRESQL_SERVER.getEffectiveDefaultValue(Constants.EMPTY_STRING));
        proxyServer = PROXY_SERVER.getValue(props, PROXY_SERVER.getEffectiveDefaultValue(Constants.EMPTY_STRING));

        String url = Option.SERVER_URL.getValue(props,
                Option.SERVER_URL.getEffectiveDefaultValue(Constants.EMPTY_STRING));
        // https://java.testcontainers.org/features/networking/#exposing-host-ports-to-the-container?
        if (Checker.isNullOrEmpty(url)) {
            // wild guess since host.docker.internal does not work on Linux by default
            url = Utils.getHost("172.17.0.1");
            serverUrl = Utils.format("http://%s:%s%s", Option.SERVER_HOST.getValue(props, url),
                    Option.SERVER_PORT.getValue(props, Option.SERVER_PORT.getEffectiveDefaultValue("8080")),
                    Option.SERVER_CONTEXT.getValue(props, Option.SERVER_CONTEXT.getEffectiveDefaultValue("/")));
        } else {
            serverUrl = url;
        }

        if (!Checker.isNullOrEmpty(clickhouseServer) && !Checker.isNullOrEmpty(postgresqlServer)
                && !Checker.isNullOrEmpty(proxyServer)) {
            containers = null;
        } else {
            final File file = new File("target/test-classes/docker-compose.yml");
            if (!file.exists()) {
                try (InputStream in = BaseIntegrationTest.class.getClassLoader()
                        .getResourceAsStream("docker-compose.yml"); OutputStream out = new FileOutputStream(file)) {
                    Stream.pipe(in, out);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            containers = new DockerComposeContainer<>(file)
                    .withExposedService(CLICKHOUSE_SERVICE, CLICKHOUSE_PORT,
                            Wait.forHttp("/ping").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(30)))
                    .withExposedService(POSTGRESQL_SERVICE, POSTGRESQL_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)))
                    .withExposedService(PROXY_SERVICE, CLICKHOUSE_PORT)
                    .withExposedService(PROXY_SERVICE, PROXY_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)))
                    .withLocalCompose(false);
        }
    }

    public static String getDeclaredClickHouseServer() {
        if (Checker.isNullOrEmpty(clickhouseServer)) {
            return new StringBuilder(CLICKHOUSE_SERVICE).append(':').append(CLICKHOUSE_PORT).toString();
        }
        return clickhouseServer;
    }

    public static String getDeclaredPostgreSqlServer() {
        if (Checker.isNullOrEmpty(postgresqlServer)) {
            return new StringBuilder(POSTGRESQL_SERVICE).append(':').append(POSTGRESQL_PORT).toString();
        }
        return postgresqlServer;
    }

    public static String getClickHouseServer() {
        if (Checker.isNullOrEmpty(clickhouseServer)) {
            return new StringBuilder(containers.getServiceHost(CLICKHOUSE_SERVICE, CLICKHOUSE_PORT)).append(':')
                    .append(containers.getServicePort(CLICKHOUSE_SERVICE, CLICKHOUSE_PORT)).toString();
        }
        return clickhouseServer;
    }

    public static String getPostgreSqlServer() {
        if (Checker.isNullOrEmpty(postgresqlServer)) {
            return new StringBuilder(containers.getServiceHost(POSTGRESQL_SERVICE, POSTGRESQL_PORT)).append(':')
                    .append(containers.getServicePort(POSTGRESQL_SERVICE, POSTGRESQL_PORT)).toString();
        }
        return postgresqlServer;
    }

    public static String getProxyControlServer() {
        if (Checker.isNullOrEmpty(proxyServer)) {
            return new StringBuilder(containers.getServiceHost(PROXY_SERVICE, PROXY_PORT)).append(':')
                    .append(containers.getServicePort(PROXY_SERVICE, PROXY_PORT)).toString();
        }
        return proxyServer;
    }

    public static String getProxyServer() {
        if (Checker.isNullOrEmpty(proxyServer)) {
            return new StringBuilder(containers.getServiceHost(PROXY_SERVICE, CLICKHOUSE_PORT)).append(':')
                    .append(containers.getServicePort(PROXY_SERVICE, CLICKHOUSE_PORT)).toString();
        }
        return proxyServer;
    }

    public static String getServerUrl() {
        return serverUrl;
    }

    @BeforeSuite(groups = { "integration" })
    public static void beforeSuite() {
        if (containers != null) {
            for (String service : new String[] { CLICKHOUSE_SERVICE, POSTGRESQL_SERVICE, PROXY_SERVICE }) {
                Optional<ContainerState> state = containers.getContainerByServiceName(service);
                if (state.isPresent() && state.get().isRunning()) {
                    return;
                }
            }

            try {
                containers.start();

                ToxiproxyClient toxiproxyClient = new ToxiproxyClient(
                        containers.getServiceHost(PROXY_SERVICE, PROXY_PORT),
                        containers.getServicePort(PROXY_SERVICE, PROXY_PORT));
                toxiproxyClient.createProxy(CLICKHOUSE_SERVICE, "0.0.0.0:" + CLICKHOUSE_PORT,
                        getDeclaredClickHouseServer());
            } catch (Exception e) {
                throw new IllegalStateException(new StringBuilder()
                        .append("Failed to start docker container for integration test.\r\n")
                        .append("If you prefer to run tests without docker, ")
                        .append("please follow instructions at https://github.com/jdbcx/jdbcx#testing")
                        .toString(), e);
            }
        }
    }

    @AfterSuite(groups = { "integration" })
    public static void afterSuite() {
        if (containers != null) {
            containers.stop();
        }
    }
}
