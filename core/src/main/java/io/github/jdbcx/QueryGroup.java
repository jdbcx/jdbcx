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
package io.github.jdbcx;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * This class represents a group of queries.
 */
public final class QueryGroup {
    static final String QUERY_DELIMITER = "--;; ";

    static final String PREFIX = QueryGroup.class.getSimpleName() + " #";
    static final String SOURCE = " in [";
    static final int BUFFER = PREFIX.length() + SOURCE.length() + 5;

    /**
     * Splits the query into zero or more groups separated by
     * {@code ^--;; <comments>$}.
     *
     * @param query the original query
     * @return non-null list of query groups
     */
    public static List<QueryGroup> of(String query) {
        if (Checker.isNullOrBlank(query)) {
            return Collections.emptyList();
        }

        final int dlen = QUERY_DELIMITER.length();
        final int len = query.length();
        final List<QueryGroup> list = new LinkedList<>();

        int lastIndex = 0;
        String comment = Constants.EMPTY_STRING;
        for (int i = 0; i < len; i++) {
            int index = query.indexOf(QUERY_DELIMITER, i);
            if (index == -1) { // last group
                String subQuery = query.substring(i).trim();
                if (!subQuery.isEmpty()) {
                    list.add(new QueryGroup(list.size(), comment, subQuery));
                }
                lastIndex = -1;
            } else if (index == lastIndex || query.charAt(index - 1) == '\n') { // new group
                if (index > lastIndex) { // might be a valid query
                    String subQuery = query.substring(lastIndex, index - 1).trim();
                    if (!subQuery.isEmpty()) {
                        list.add(new QueryGroup(list.size(), comment, subQuery));
                    }
                }
                index += dlen;
                // now update comment
                int idx = query.indexOf('\n', index);
                if (idx != -1) {
                    comment = query.substring(index, idx).trim();
                    i = idx; // NOSONAR
                    lastIndex = idx + 1;
                } else {
                    comment = query.substring(index).trim();
                    lastIndex = -1;
                }
            }

            if (lastIndex == -1) {
                break;
            }
        }

        if (lastIndex > 0) {
            String subQuery = query.substring(lastIndex).trim();
            if (!subQuery.isEmpty()) {
                list.add(new QueryGroup(list.size(), comment, subQuery));
            }
        }

        return Collections.unmodifiableList(list);
    }

    private final int index;
    private final String description;
    private final String query;

    public QueryGroup(int index, String description, String query) {
        this.index = index >= 0 ? index : 0;
        this.description = description != null ? description : Constants.EMPTY_STRING;
        this.query = query != null ? query : Constants.EMPTY_STRING;
    }

    public int getIndex() {
        return index;
    }

    public String getDescription() {
        return description;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = index;
        result = prime * result + description.hashCode();
        result = prime * result + query.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryGroup other = (QueryGroup) obj;
        return index == other.index && description.equals(other.description) && query.equals(other.query);
    }

    public String toString(String source) {
        final int descLength = description.length();
        final int srcLength = source.length();

        final StringBuilder builder = new StringBuilder(BUFFER + descLength + srcLength);
        builder.append(PREFIX).append(index + 1);
        if (descLength > 0) {
            builder.append(' ').append(description);
        }
        if (srcLength > 0) {
            builder.append(SOURCE).append(source).append(']');
        }
        return builder.toString();
    }
}
