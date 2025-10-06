/*
 * Copyright 2022-2025, Zhichun Wu
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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * This class represents a query task.
 */
public final class QueryTask {
    public static List<QueryTask> ofQuery(String query) {
        final List<QueryTask> list = new LinkedList<>();
        for (QueryGroup g : QueryGroup.of(query)) {
            list.add(new QueryTask(Constants.EMPTY_STRING, Collections.singletonList(g)));
        }
        return list.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(list);
    }

    public static List<QueryTask> ofFile(String file, Charset charset) {
        if (Checker.isNullOrEmpty(file)) {
            return Collections.emptyList();
        }

        try {
            final List<QueryTask> list = new LinkedList<>();
            for (QueryGroup g : QueryGroup.of(Stream.readAllAsString(new FileInputStream(file), charset))) {
                list.add(new QueryTask(file, Collections.singletonList(g)));
            }
            return list.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(list);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static List<QueryTask> ofPath(String path, Charset charset) {
        final List<Path> paths;
        try {
            paths = Utils.findFiles(path, null);
        } catch (IOException e) {
            throw new IllegalArgumentException(Utils.format("Failed to extract files from given path [%s]", path),
                    e);
        }

        final int pathCount = paths.size();
        final List<QueryTask> tasks;
        if (pathCount == 0) {
            tasks = Collections.emptyList();
        } else if (pathCount == 1) {
            tasks = ofFile(paths.get(0).toString(), charset);
        } else {
            List<QueryTask> list = new ArrayList<>(pathCount);
            try {
                for (Path p : paths) {
                    String file = p.toString();
                    list.add(new QueryTask(file,
                            QueryGroup.of(Stream.readAllAsString(new FileInputStream(file), charset))));
                }
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
            tasks = list.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(list);
        }
        return tasks;
    }

    private final String source;
    private final List<QueryGroup> groups;

    public QueryTask(String source, List<QueryGroup> groups) {
        this.source = source != null ? source : Constants.EMPTY_STRING;
        this.groups = groups != null ? groups : Collections.emptyList();
    }

    public String getSource() {
        return source;
    }

    public List<QueryGroup> getGroups() {
        return groups;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = source.hashCode();
        result = prime * result + groups.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryTask other = (QueryTask) obj;
        return source.equals(other.source) && groups.equals(other.groups);
    }
}
