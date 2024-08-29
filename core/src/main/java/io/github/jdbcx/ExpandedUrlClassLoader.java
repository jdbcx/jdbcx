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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is an enhanced version of the {@link URLClassLoader} that provides
 * support for loading classes from local directories.
 */
public final class ExpandedUrlClassLoader extends URLClassLoader {
    static final class Fingerprint {
        private final ClassLoader parent;
        private final String[] urls;

        Fingerprint(ClassLoader parent, String[] urls) {
            this.parent = parent;
            this.urls = urls;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = prime + ((parent == null) ? 0 : parent.hashCode());
            result = prime * result + Arrays.hashCode(urls);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            Fingerprint other = (Fingerprint) obj;
            return Objects.equals(parent, other.parent) && Arrays.equals(urls, other.urls);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ExpandedUrlClassLoader.class);

    private static final Option OPTION_CACHE_SIZE = Option.of("classloader.cache.size", "Class loader cache size",
            "100");
    private static final Option OPTION_CACHE_EXPTIME = Option.of("classloader.cache.exptime",
            "Class loader cache expiration time in second", "0");

    private static final Cache<Fingerprint, ClassLoader> cache = Cache.create(
            Integer.parseInt(OPTION_CACHE_SIZE.getEffectiveDefaultValue(Option.PROPERTY_PREFIX)),
            Integer.parseInt(OPTION_CACHE_EXPTIME.getEffectiveDefaultValue(Option.PROPERTY_PREFIX)),
            ExpandedUrlClassLoader::new);

    static final String CLASS_PATH_DELIMITER = "||";
    static final String PROTOCOL_FILE = "file";
    static final String DRIVER_EXTENSION = ".jar";

    public static final ClassLoader of(ClassLoader parent, String... urls) {
        if (urls == null || urls.length == 0) {
            return parent != null ? parent : ExpandedUrlClassLoader.class.getClassLoader();
        }

        return cache
                .get(new Fingerprint(parent != null ? parent : ExpandedUrlClassLoader.class.getClassLoader(), urls));
    }

    public static final ClassLoader of(Class<?> callerClass, String... urls) {
        if (urls == null || urls.length == 0) {
            return getSuitableClassLoader(callerClass);
        }

        return cache.get(new Fingerprint(getSuitableClassLoader(callerClass), urls));
    }

    protected static URL[] expandURLs(String... urls) { // NOSONAR
        if (urls == null || urls.length == 0) {
            urls = new String[] { Constants.CURRENT_DIR };
        }
        Set<String> cache = new HashSet<>();
        List<URL> list = new ArrayList<>(urls.length * 2);
        Set<URL> negativeSet = new HashSet<>(); // NOSONAR

        for (String s : urls) {
            if (s == null || s.isEmpty() || cache.contains(s)) {
                continue;
            }

            boolean isNegative = s.length() > 1 && s.charAt(0) == '!';
            if (isNegative) {
                s = s.substring(1);
            }
            if (s.startsWith("~/")) {
                s = Constants.HOME_DIR.concat(s.substring(1));
            }
            URL url = null;
            try {
                url = cache.add(s) ? Utils.toURL(s) : null;
            } catch (MalformedURLException e) {
                // might be a local path?
                try {
                    URL tmp = Paths.get(s).normalize().toUri().toURL();
                    if (cache.add(s = tmp.toString())) {
                        url = tmp;
                    }
                } catch (InvalidPathException exp) {
                    log.warn("Skip invalid path [%s]", s);
                } catch (MalformedURLException exp) {
                    log.warn("Skip malformed URL [%s]", s);
                }
            }

            if (url == null) {
                continue;
            }

            boolean isValid = true;
            if (PROTOCOL_FILE.equals(url.getProtocol())) {
                Path path = null;
                try {
                    path = Paths.get(url.toURI());
                } catch (URISyntaxException e) {
                    isValid = false;
                    log.warn("Skip invalid URL [%s]", url);
                } catch (InvalidPathException e) {
                    isValid = false;
                    log.warn("Skip invalid path [%s]", url);
                }

                if (path != null && Files.isDirectory(path)) {
                    File dir = path.normalize().toFile();
                    String[] files = dir.list();
                    Arrays.sort(files);
                    for (String file : files) {
                        if (file.endsWith(DRIVER_EXTENSION)) {
                            file = new StringBuilder(dir.getPath()).append(File.separatorChar).append(file).toString();

                            if (isNegative) {
                                try {
                                    negativeSet.add(Utils.toURL(file));
                                } catch (Exception e) {
                                    // ignore
                                }
                            } else if (cache.add(file)) {
                                try {
                                    list.add(Utils.toURL(file));
                                } catch (MalformedURLException e) {
                                    log.warn("Skip invalid file [%s]", file);
                                }
                            } else {
                                log.warn("Discard duplicated file [%s]", file);
                            }
                        }
                    }
                }
            }

            if (isValid) {
                (isNegative ? negativeSet : list).add(url);
            }
        }

        if (list.removeAll(negativeSet)) {
            log.debug("Excluded URLs: %s", negativeSet);
        }

        return list.toArray(new URL[0]);
    }

    /**
     * This method tries to find the most suitable class loader to use. It starts
     * with {@link Thread#getContextClassLoader()}, and then the loader of
     * {@code callerClass}, lastly the loader of this class.
     * 
     * @param callerClass optional caller class
     * @return class loader
     */
    protected static ClassLoader getSuitableClassLoader(Class<?> callerClass) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            if (callerClass != null) {
                loader = callerClass.getClassLoader();
            }

            if (loader == null) {
                loader = ExpandedUrlClassLoader.class.getClassLoader();
            }
        }
        return loader;
    }

    private final Fingerprint fingerprint;
    private final Map<String, Class<?>> loadedClasses;

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        Class<?> clazz = loadedClasses.get(name); // NOSONAR
        if (clazz == null) {
            clazz = super.findClass(name);
            loadedClasses.put(name, clazz);
        }
        return clazz;
    }

    protected ExpandedUrlClassLoader(Fingerprint fingerprint) {
        super(expandURLs(fingerprint.urls), fingerprint.parent);

        this.fingerprint = fingerprint;
        loadedClasses = new ConcurrentHashMap<>();
    }

    public String getOriginalUrls() {
        return Arrays.toString(fingerprint.urls);
    }

    @Override
    public void close() throws IOException {
        this.loadedClasses.clear();
        try {
            super.close();
        } finally {
            cache.invalidate(fingerprint);
        }
    }

    @Override
    public String toString() {
        return new StringBuilder(super.toString()).append("(parent=").append(fingerprint.parent).append("urls=")
                .append(Arrays.toString(fingerprint.urls)).append(')').toString();
    }
}
