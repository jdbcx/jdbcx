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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

public final class ErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    public ErrorHandler attach(AutoCloseable... resources) {
        if (resources != null) {
            for (AutoCloseable res : resources) {
                if (res != null && !this.resources.contains(res)) {
                    this.resources.add(res);
                }
            }
        }
        return this;
    }

    public InputStream handle(Throwable error, String charset) throws CompletionException {
        return new ByteArrayInputStream(handle(error).getBytes(Charset.forName(charset)));
    }

    public String handle(Throwable error) throws CompletionException {
        try { // NOSONAR
            final String errorHandling = Option.EXEC_ERROR.getValue(props);
            if (Option.ERROR_HANDLING_RETURN.equals(errorHandling)) {
                log.debug("Failed to interpret. Will return error message as result.", error);
                return error.getMessage();
            } else if (Option.ERROR_HANDLING_IGNORE.equals(errorHandling)) {
                log.debug("Failed to interpret. Will ignore.", error);
            } else if (Option.ERROR_HANDLING_WARN.equals(errorHandling) && !(error instanceof TimeoutException)) {
                log.debug("Failed to interpret. Will raise a warning.", error);
                throw new CompletionException(error.getMessage(), null);
            } else {
                throw new CompletionException(error);
            }
            return query;
        } finally {
            for (AutoCloseable res : resources) {
                try {
                    res.close();
                } catch (Throwable t) { // NOSONAR
                    log.debug("Failed to close resource [%s] due to ", res, t.getMessage());
                }
            }
        }
    }

    private final String query;
    private final Properties props;
    private final List<AutoCloseable> resources;

    public ErrorHandler(String query) {
        this(query, null);
    }

    public ErrorHandler(String query, Properties props) {
        this.query = query != null ? query : Constants.EMPTY_STRING;
        this.props = new Properties(props);
        this.resources = Collections.synchronizedList(new ArrayList<>());
    }
}
