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

import java.util.Properties;
import java.util.concurrent.CompletionException;

public interface Interpreter {
    /**
     * Gets context bound to current thread.
     *
     * @return context bound to current thread
     */
    default QueryContext getContext() {
        return QueryContext.getCurrentContext();
    }

    /**
     * Gets variable tag can be used in query.
     *
     * @return non-null variable tag
     */
    default VariableTag getVariableTag() {
        return VariableTag.BRACE;
    }

    /**
     * Interprets the given query.
     *
     * @param query non-null query
     * @param props optional properties
     * @return non-null result
     * @throws CompletionException when failed to interpret the query
     */
    Result<?> interpret(String query, Properties props);
}
