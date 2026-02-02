/*
 * Copyright 2022-2026, Zhichun Wu
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
package io.github.jdbcx.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
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
import io.github.jdbcx.ConfigManager;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;
import io.github.jdbcx.VariableTag;

public class PropertyFileConfigManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(PropertyFileConfigManager.class);

    static final Option OPTION_BASE_DIR = Option
            .of(new String[] { "base.dir", "Base directory without backslash suffix for finding named connections",
                    Constants.CONF_DIR });
    static final String FILE_EXTENSION = ".properties";

    private final AtomicReference<Thread> monitor;

    private final Set<String> categories;
    private final Map<String, Properties> config;

    private final AtomicReference<Path> path;
    private final AtomicBoolean managed;

    public PropertyFileConfigManager(Properties props) {
        super(props);
        monitor = new AtomicReference<>();

        categories = Collections.synchronizedSet(new LinkedHashSet<>());
        config = new ConcurrentHashMap<>();

        path = new AtomicReference<>(Utils.getPath(OPTION_BASE_DIR.getJdbcxValue(props), true));
        managed = new AtomicBoolean(false);
    }

    void load() {
        if (!managed.get()) {
            categories.clear();
            config.clear();
            return;
        }

        final Path baseDir = path.get();
        log.debug("Loading named connections from [%s]", baseDir);
        Set<String> ids = new HashSet<>();
        try {
            File[] files = baseDir.toFile().listFiles();
            if (files == null) {
                config.clear();
                log.debug("Deleted configuration cache to force re-initialization.");
                return;
            }

            for (File f : files) {
                if (f.isDirectory() && f.canRead()) {
                    String category = f.getName();
                    categories.add(category);
                    for (File file : f.listFiles()) {
                        if (file.isFile() && file.canRead()) {
                            String fileName = file.getName();
                            if (fileName.endsWith(FILE_EXTENSION)) {
                                String id = fileName.substring(0, fileName.length() - FILE_EXTENSION.length());
                                if (id.indexOf('.') != -1) {
                                    log.debug("Skip configuration [%s] as dot is a reserved character", id);
                                    continue;
                                }

                                String uid = getUniqueId(category, id);
                                log.debug("Loading configuration [%s] from [%/s%s]...", id, category, file);
                                try (Reader reader = new InputStreamReader(new FileInputStream(file),
                                        Constants.DEFAULT_CHARSET)) {
                                    Properties properties = new Properties();
                                    properties.load(reader);
                                    decrypt(properties, id);
                                    ids.add(uid);
                                    config.put(uid, properties);
                                    Option.ID.setJdbcxValue(properties, id);
                                } catch (IOException e) {
                                    log.debug("Failed to load configuration from [%s/%s]", category, file, e);
                                }
                            }
                        }
                    }
                }
            }

            // now aliases
            for (Properties props : config.values()) {
                String id = Option.ID.getJdbcxValue(props);
                String category = id.split("/")[0];
                for (String alias : OPTION_ALIAS.getJdbcxValue(props).split(",")) {
                    if (!Checker.isNullOrBlank(alias)) {
                        String uid = getUniqueId(category, alias.trim());
                        Properties p = config.get(uid);
                        String legacyId = Constants.EMPTY_STRING;
                        if (p != null && config.containsKey(legacyId = Option.ID.getJdbcxValue(p))
                                && legacyId.equals(uid)) {
                            log.warn("Skip alias [%s] as it's been taken", uid);
                        } else { // either new entry or an alias, which is safe to override
                            ids.add(uid);
                            config.put(uid, props);
                            log.debug("Added alias [%s]", uid);
                        }
                    }
                }
            }

            for (Iterator<Entry<String, Properties>> it = config.entrySet().iterator(); it.hasNext();) {
                Entry<String, Properties> entry = it.next();
                if (!ids.contains(entry.getKey())) {
                    it.remove();
                    log.debug("Removed configuration [%s]", entry.getKey());
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
    public List<String> getAllIDs(String category) {
        if (managed.get()) {
            final List<String> matched;
            if (Checker.isNullOrEmpty(category)) {
                matched = Collections.unmodifiableList(new ArrayList<>(config.keySet()));
            } else {
                String prefix = new StringBuilder(category).append('/').toString();
                List<String> list = new LinkedList<>();
                for (String id : config.keySet()) {
                    if (id.startsWith(prefix)) {
                        list.add(id);
                    }
                }
                matched = list.isEmpty() ? Collections.emptyList()
                        : Collections.unmodifiableList(new ArrayList<>(list));
            }
            return matched;
        }

        if (Checker.isNullOrEmpty(category)) {
            return Collections.emptyList();
        }

        Path dir = path.get().resolve(category);
        if (!Files.exists(dir)) {
            return Collections.emptyList();
        }

        try (Stream<Path> s = Files.walk(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(FILE_EXTENSION))
                    .map(p -> p.getFileName().toString().replace(FILE_EXTENSION, Constants.EMPTY_STRING))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean hasConfig(String category, String id) {
        final String uniqueId = getUniqueId(category, id);

        return managed.get() ? config.containsKey(uniqueId)
                : Files.exists(path.get().resolve(uniqueId.concat(FILE_EXTENSION)));
    }

    @Override
    public Properties getConfig(String category, String id, VariableTag tag, String tenant) {
        if (Checker.isNullOrEmpty(category) || Checker.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("Non-empty category and id are required");
        }

        Properties props;
        if (managed.get()) {
            props = config.get(getUniqueId(category, id));
            if (props != null) {
                Properties newProps = new Properties();
                newProps.putAll(props);
                props = newProps;
            }
        } else {
            props = new Properties();
            final Path baseDir = path.get();
            Path p = baseDir.resolve(getUniqueId(category, id).concat(FILE_EXTENSION));
            log.debug("Loading configuration of named %s [%s] from [%s]...", category, id, p);
            try (Reader reader = new InputStreamReader(new FileInputStream(p.toFile()), Constants.DEFAULT_CHARSET)) {
                props.load(reader);
                decrypt(props, id);
                props.remove(Option.ID.getJdbcxName());
                if (props.getProperty(Option.ID.getName()) != null) {
                    Option.ID.setValue(props, id);
                }
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException(
                        Utils.format("Named %s [%s] does not exist", category, id), e);
            } catch (IOException e) {
                throw new IllegalArgumentException(
                        Utils.format("Failed to load configuration of named %s [%s]", category, id), e);
            }
        }

        if (props == null) {
            throw new IllegalArgumentException(
                    Utils.format("Could not find configuration for named %s [%s]", category, id));
        } else if (tag != null && tenant != null) {
            applySecrets(tenant, tag, props);
        }
        return props;
    }

    @Override
    public void reload(Properties props) {
        final Path currentPath = path.get();
        final Path newPath = Utils.getPath(OPTION_BASE_DIR.getJdbcxValue(props), true);
        if (newPath.equals(currentPath) && (managed.get() || !managed.compareAndSet(false, true))) {
            return;
        }

        final Thread thread = monitor.get();
        if (thread != null) {
            thread.interrupt();
        }

        if (path.compareAndSet(currentPath, newPath)) {
            log.debug("Changed base directory of named connections from [%s] to [%s]", currentPath, newPath);
        } else {
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
