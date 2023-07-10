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
import java.util.Set;

/**
 * This class is an enhanced version of the {@link URLClassLoader} that provides
 * support for loading classes from local directories.
 */
public final class ExpandedUrlClassLoader extends URLClassLoader {
    static final String PROTOCOL_FILE = "file";
    static final String FILE_URL_PREFIX = PROTOCOL_FILE + ":///";
    static final String DRIVER_EXTENSION = ".jar";

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
                url = cache.add(s) ? new URL(s) : null;
            } catch (MalformedURLException e) {
                // might be a local path?
                try {
                    URL tmp = Paths.get(s).normalize().toUri().toURL();
                    if (cache.add(tmp.toString())) {
                        url = tmp;
                    }
                } catch (InvalidPathException | MalformedURLException exp) {
                    // ignore
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
                } catch (InvalidPathException | URISyntaxException e) {
                    isValid = false;
                }

                if (path != null && Files.isDirectory(path)) {
                    File dir = path.normalize().toFile();
                    String[] files = dir.list();
                    Arrays.sort(files);
                    for (String file : files) {
                        if (file.endsWith(DRIVER_EXTENSION)) {
                            file = new StringBuilder().append(FILE_URL_PREFIX).append(dir.getPath())
                                    .append(File.separatorChar).append(file).toString();

                            if (isNegative) {
                                try {
                                    negativeSet.add(new URL(file));
                                } catch (Exception e) {
                                    // ignore
                                }
                            } else if (cache.add(file)) {
                                try {
                                    list.add(new URL(file));
                                } catch (MalformedURLException e) {
                                    // ignore
                                }
                            }
                        }
                    }
                }
            }

            if (isValid) {
                (isNegative ? negativeSet : list).add(url);
            }
        }

        list.removeAll(negativeSet);

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

    private final String originalUrls;

    public ExpandedUrlClassLoader(Class<?> callerClass, String urls) {
        this(getSuitableClassLoader(callerClass), urls);
    }

    public ExpandedUrlClassLoader(ClassLoader parent, String urls) {
        super(expandURLs(urls), parent == null ? ExpandedUrlClassLoader.class.getClassLoader() : parent);

        this.originalUrls = urls;
    }

    public String getOriginalUrls() {
        return originalUrls;
    }
}
