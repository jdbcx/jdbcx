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
package io.github.jdbcx.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.executor.jdbc.SqlExceptionUtils;
import io.github.jdbcx.interpreter.JdbcConnectionManager;

public class FileBasedJdbcConnectionManager extends JdbcConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(FileBasedJdbcConnectionManager.class);

    static final Option OPTION_DIRECTORY = Option.of(
            new String[] { "directory", "Configuration directory without backslash suffix", "~/.jdbcx/connections" });
    static final String FILE_EXTENSION = ".properties";

    private final AtomicReference<Thread> monitor;
    private final Map<String, Properties> config;

    private final AtomicReference<Path> path;
    private final AtomicBoolean managed;

    public FileBasedJdbcConnectionManager() {
        monitor = new AtomicReference<>();
        config = new ConcurrentHashMap<>();

        path = new AtomicReference<>(Utils.getPath(OPTION_DIRECTORY.getDefaultValue(), true));
        managed = new AtomicBoolean(false);
    }

    void load() {
        if (!managed.get()) {
            config.clear();
            return;
        }
        Set<String> ids = new HashSet<>();
        try {
            File[] files = path.get().toFile().listFiles();
            if (files == null) {
                config.clear();
                log.debug("Emptyed all connections");
                return;
            }

            for (File file : files) {
                if (file.isFile() && file.canRead()) {
                    String fileName = file.getName();
                    if (fileName.endsWith(FILE_EXTENSION)) {
                        String id = fileName.substring(0, fileName.length() - FILE_EXTENSION.length());
                        log.debug("Loading connection [%s] from [%s]...", id, file);
                        try (Reader reader = new InputStreamReader(new FileInputStream(file),
                                Constants.DEFAULT_CHARSET)) {
                            Properties properties = new Properties();
                            properties.load(reader);
                            ids.add(id);
                            config.put(id, properties);
                        } catch (IOException e) {
                            log.debug("Failed to load [%s]", file, e);
                        }
                    }
                }
            }

            // now aliases
            for (Properties props : config.values()) {
                for (String alias : OPTION_ALIAS.getJdbcxValue(props).split(",")) {
                    if (!Checker.isNullOrBlank(alias)) {
                        String newId = alias.trim();
                        Properties p = config.get(newId);
                        String legacyId = Constants.EMPTY_STRING;
                        if (p != null && config.containsKey(legacyId = OPTION_ID.getJdbcxValue(p))
                                && legacyId.equals(newId)) {
                            log.warn("Skip alias [%s] as it's been taken", newId);
                        } else { // either new entry or an alias, which is safe to override
                            ids.add(newId);
                            config.put(newId, props);
                            log.debug("Added alias [%s]", newId);
                        }
                    }
                }
            }

            for (Iterator<Entry<String, Properties>> it = config.entrySet().iterator(); it.hasNext();) {
                Entry<String, Properties> entry = it.next();
                if (!ids.contains(entry.getKey())) {
                    it.remove();
                    log.debug("Removed connection [%s]", entry.getKey());
                }
            }
        } catch (Exception e) {
            log.error("Failed to get list of configuration files under [%s]", path, e);
        }
    }

    void monitor() {
        if (!managed.get()) {
            return;
        }
        try {
            WatchService watcher = FileSystems.getDefault().newWatchService();
            path.get().register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE);
            log.debug("Start to monitor changes under [%s]...", path);
            while (true) {
                WatchKey key = watcher.take();

                for (WatchEvent<?> event : key.pollEvents()) {
                    log.debug("%s - %s", event.kind(), event.context());
                }

                if (!key.reset()) {
                    log.error("Invalid key, stopped monitoring [%s]", path);
                    break;
                }
            }
        } catch (IOException e) {
            log.error("Failed to monitor path [%s]", path, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public List<String> getAllConnectionIds() {
        if (managed.get()) {
            return Collections.unmodifiableList(new ArrayList<>(config.keySet()));
        }

        try (Stream<Path> s = Files.walk(path.get())) {
            return s.filter(p -> p.getFileName().toString().endsWith(FILE_EXTENSION))
                    .map(p -> p.getFileName().toString().replace(FILE_EXTENSION, Constants.EMPTY_STRING))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Connection getConnection(String id) throws SQLException {
        Properties props;
        if (managed.get()) {
            props = config.get(id);
        } else {
            props = new Properties();
            Path p = path.get().resolve(id + FILE_EXTENSION);
            log.debug("Loading connection [%s] from [%s]...", id, p);
            try (Reader reader = new InputStreamReader(new FileInputStream(p.toFile()), Constants.DEFAULT_CHARSET)) {
                props.load(reader);
            } catch (IOException e) {
                throw SqlExceptionUtils.clientError(e);
            }
        }
        if (props == null) {
            throw SqlExceptionUtils.clientError(Utils.format("Could not find connection [%s]", id));
        }
        final String cid = OPTION_ID.getJdbcxValue(props);
        final String url = OPTION_URL.getJdbcxValue(props);

        if (!cid.equals(id)) {
            throw SqlExceptionUtils.clientError(Utils.format(
                    "Inconsistent connection ID - it's [%s] in configuration file, but [%s] in request", cid, id));
        } else if (Checker.isNullOrBlank(url)) {
            throw SqlExceptionUtils.clientError("No connection URL found");
        }

        log.debug("Connecting to [%s]...", id);
        Properties newProps = new Properties();
        newProps.putAll(props);
        return getConnection(url, newProps, null); // getConnection(url, new Properties(props));
    }

    @Override
    public void reload(Properties props) {
        final Path currentPath = path.get();
        final Path newPath = Utils.getPath(OPTION_DIRECTORY.getJdbcxValue(props), true);
        if (newPath.equals(currentPath) && (managed.get() || !managed.compareAndSet(false, true))) {
            return;
        }

        final Thread thread = monitor.get();
        if (thread != null) {
            thread.interrupt();
        }

        if (!path.compareAndSet(currentPath, newPath)) {
            log.warn("Failed to change connection configuration path from [%s] to [%s], could be a problem",
                    currentPath, newPath);
        }

        load();

        final Thread newThread = new Thread(this::monitor);
        newThread.setDaemon(true);
        if (monitor.compareAndSet(thread, newThread)) {
            newThread.start();
        }
    }
}
