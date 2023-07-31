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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.interpreter.JdbcConnectionManager;

public class FileBasedJdbcConnectionManager extends JdbcConnectionManager {
    private static final Logger log = LoggerFactory.getLogger(FileBasedJdbcConnectionManager.class);

    static final Option OPTION_DIRECTORY = Option.of(
            new String[] { "directory", "Configuration directory without backslash suffix", "~/.jdbcx/connections" });
    static final String FILE_EXTENSION = ".properties";

    private final AtomicReference<Thread> monitor;
    private final Map<String, Properties> config;

    private final AtomicBoolean cache;
    private final AtomicReference<Path> path;

    public FileBasedJdbcConnectionManager() {
        monitor = new AtomicReference<>();
        config = new ConcurrentHashMap<>();

        cache = new AtomicBoolean(false);
        path = new AtomicReference<>(Utils.getPath(OPTION_DIRECTORY.getDefaultValue(), true));
    }

    void load() {
        if (!cache.get()) {
            config.clear();
            return;
        }

        Set<String> ids = new HashSet<>();
        try {
            for (File file : path.get().toFile().listFiles()) {
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

            for (Iterator<Entry<String, Properties>> it = config.entrySet().iterator(); it.hasNext();) {
                Entry<String, Properties> entry = it.next();
                if (!ids.contains(entry.getKey())) {
                    it.remove();
                }
            }
        } catch (Exception e) {
            log.error("Failed to get list of configuration files under [%s]", path, e);
        }
    }

    void monitor() {
        if (!cache.get()) {
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
    public Connection getConnection(String id) throws SQLException {
        Properties props;
        if (cache.get()) {
            props = config.get(id);
        } else {
            props = new Properties();
            Path p = path.get().resolve(id + FILE_EXTENSION);
            log.debug("Loading connection [%s] from [%s]...", id, p);
            try (InputStream in = new FileInputStream(p.toFile())) {
                props.load(in);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        if (props == null) {
            throw new SQLException(Utils.format("Could not find connection [%s]", id));
        }
        final String cid = OPTION_ID.getJdbcxValue(props);
        final String url = OPTION_URL.getJdbcxValue(props);

        if (!cid.equals(id)) {
            throw new SQLException(Utils.format(
                    "Inconsistent connection ID - it's [%s] in configuration file, but [%s] in request", cid, id));
        } else if (Checker.isNullOrBlank(url)) {
            throw new SQLException("No connection URL found");
        }

        log.debug("Connecting to [%s]...", id);
        return getConnection(url, new Properties(props));
    }

    public void reload(Properties props) {
        final Thread thread = monitor.get();
        if (thread != null) {
            thread.interrupt();
        }

        path.set(Utils.getPath(OPTION_DIRECTORY.getJdbcxValue(props), true));

        load();

        final Thread newThread = new Thread(this::monitor);
        newThread.setDaemon(true);
        if (monitor.compareAndSet(thread, newThread)) {
            newThread.start();
        }
    }
}
