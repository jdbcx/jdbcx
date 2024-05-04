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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.ComposeContainer;
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
    static final class ServiceConfig {
        final String name;
        final int port;
        final String healthCheck;
        final boolean container;
        final String address;

        ServiceConfig(String name, int port, Properties props) {
            this(name, port, null, props);
        }

        ServiceConfig(String name, int port, String heathCheck, Properties props) {
            this.name = name;
            this.port = port;
            this.healthCheck = heathCheck == null ? Constants.EMPTY_STRING : heathCheck;

            Option option = Option.of(new String[] { name + ".server", name + " server address", "" });
            String value = option.getValue(props, option.getEffectiveDefaultValue(Constants.EMPTY_STRING));
            if (Checker.isNullOrEmpty(value)) {
                this.container = true;
                this.address = new StringBuilder(name).append(':').append(port).toString();
            } else {
                this.container = false;
                this.address = value;
            }
        }

        String getAddress(ComposeContainer containers) {
            return container
                    ? new StringBuilder(containers.getServiceHost(name, port)).append(':')
                            .append(containers.getServicePort(name, port)).toString()
                    : address;
        }
    }

    private static final ServiceConfig CLICKHOUSE;
    private static final ServiceConfig FLIGHTSQL;
    private static final ServiceConfig MARIADB;
    private static final ServiceConfig MYSQL;
    private static final ServiceConfig POSTGRESQL;
    private static final ServiceConfig PROXY; // control port

    private static final List<ServiceConfig> services;
    private static final ComposeContainer containers;

    private static final String serverUrl;

    static {
        Properties props = new Properties();
        try (InputStream in = Utils.getFileInputStream("test.properties")) {
            props.load(in);
        } catch (Exception e) {
            // ignore
        }

        List<ServiceConfig> list = new ArrayList<>();
        list.add(CLICKHOUSE = new ServiceConfig("clickhouse", 8123, "/ping", props));
        list.add(FLIGHTSQL = new ServiceConfig("flightsql", 31337, props));
        list.add(MARIADB = new ServiceConfig("mariadb", 3306, props));
        list.add(MYSQL = new ServiceConfig("mysql", 3306, props));
        list.add(POSTGRESQL = new ServiceConfig("postgresql", 5432, props));
        list.add(PROXY = new ServiceConfig("toxiproxy", 8474, props)); // control port
        services = Collections.unmodifiableList(new ArrayList<>(list));

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

        if (!services.stream().anyMatch(s -> s.container)) {
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

            final int defaultTimeout = 30; // seconds
            ComposeContainer cc = new ComposeContainer(file);
            for (ServiceConfig s : services) {
                cc = cc.withExposedService(s.name, s.port,
                        s.healthCheck.isEmpty()
                                ? Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(defaultTimeout))
                                : Wait.forHttp(s.healthCheck).forStatusCode(200)
                                        .withStartupTimeout(Duration.ofSeconds(defaultTimeout)));
            }
            containers = cc.withExposedService(PROXY.name, CLICKHOUSE.port).withLocalCompose(false);
        }
    }

    public static String getDeclaredClickHouseServer() {
        return CLICKHOUSE.address;
    }

    public static String getClickHouseServer() {
        return CLICKHOUSE.getAddress(containers);
    }

    public static String getFlightSqlServer() {
        return FLIGHTSQL.getAddress(containers);
    }

    public static String getMariaDbServer() {
        return MARIADB.getAddress(containers);
    }

    public static String getMySqlServer() {
        return MYSQL.getAddress(containers);
    }

    public static String getPostgreSqlServer() {
        return POSTGRESQL.getAddress(containers);
    }

    public static String getProxyControlServer() {
        return PROXY.getAddress(containers);
    }

    public static String getProxyServer() {
        return PROXY.container && CLICKHOUSE.container
                ? new StringBuilder(containers.getServiceHost(PROXY.name, CLICKHOUSE.port)).append(':')
                        .append(containers.getServicePort(PROXY.name, CLICKHOUSE.port)).toString()
                : PROXY.address;
    }

    public static String getServerUrl() {
        return serverUrl;
    }

    @BeforeSuite(groups = { "integration" })
    public static void beforeSuite() {
        if (containers != null) {
            for (ServiceConfig s : services) {
                Optional<ContainerState> state = containers.getContainerByServiceName(s.name);
                if (state.isPresent() && state.get().isRunning()) {
                    return;
                }
            }

            try {
                containers.start();

                ToxiproxyClient toxiproxyClient = new ToxiproxyClient(containers.getServiceHost(PROXY.name, PROXY.port),
                        containers.getServicePort(PROXY.name, PROXY.port));
                toxiproxyClient.createProxy(CLICKHOUSE.name, "0.0.0.0:" + CLICKHOUSE.port,
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
