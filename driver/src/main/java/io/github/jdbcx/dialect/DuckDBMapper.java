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
package io.github.jdbcx.dialect;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Field;
import io.github.jdbcx.Format;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Result;
import io.github.jdbcx.ResultMapper;

public class DuckDBMapper implements ResultMapper {
    private static final Logger log = LoggerFactory.getLogger(DuckDBMapper.class);

    static final String FUNC_READ_CSV = "read_csv_auto('";
    static final String FUNC_READ_JSON = "read_json_auto('";
    static final String FUNC_READ_PARQUET = "read_parquet('";

    // https://duckdb.org/docs/sql/data_types/overview.html
    static final String TYPE_BOOLEAN = "BOOLEAN"; // logical boolean (true/false)
    static final String TYPE_TINYINT = "TINYINT"; // signed one-byte integer
    static final String TYPE_SMALLINT = "SMALLINT"; // signed two-byte integer
    static final String TYPE_INTEGER = "INTEGER"; // signed four-byte integer
    static final String TYPE_BIGINT = "BIGINT"; // signed eight-byte integer
    static final String TYPE_HUGEINT = "HUGEINT"; // signed sixteen-byte integer
    static final String TYPE_REAL = "REAL"; // single precision floating-point number (4 bytes)
    static final String TYPE_DOUBLE = "DOUBLE"; // double precision floating-point number (8 bytes)
    static final String TYPE_DECIMAL = "DECIMAL"; // fixed-precision number with the given width (precision) and scale,
                                                  // defaults to prec = 18 and scale = 3

    static final String TYPE_INTERVAL = "INTERVAL"; // date / time delta
    static final String TYPE_DATE = "DATE"; // calendar date (year, month day)
    static final String TYPE_TIME = "TIME"; // time of day (ignores time zone)
    static final String TYPE_TIMETZ = "TIMETZ"; // time of day (uses time zone)
    static final String TYPE_TIMESTAMP = "TIMESTAMP"; // timestamp with nanosecond precision (ignores time zone)
    static final String TYPE_TIMESTAMPTZ = "TIMESTAMPTZ"; // timestamp (uses time zone)

    static final String TYPE_BIT = "BIT"; // string of 1s and 0s
    static final String TYPE_BLOB = "BLOB"; // variable-length binary data
    static final String TYPE_UUID = "UUID"; // UUID data type
    static final String TYPE_VARCHAR = "VARCHAR"; // variable-length character string

    static final String SUFFIX_SEC = "_S";
    static final String SUFFIX_MS = "_MS";
    static final String SUFFIX_NS = "_NS";

    protected String toStructDefinition(Field... fields) {
        final int len;
        if (fields == null || (len = fields.length) == 0) {
            return new String(new char[] { '{', '}' });
        }

        StringBuilder builder = new StringBuilder().append('{');
        Field f = fields[0];
        builder.append(quoteIdentifier(f.name())).append(':').append('\'').append(toColumnType(f)).append('\'');
        for (int i = 1; i < len; i++) {
            f = fields[i];
            builder.append(',').append(quoteIdentifier(f.name())).append(':').append('\'').append(toColumnType(f))
                    .append('\'');
        }
        return builder.append('}').toString();
    }

    @Override
    public String toColumnType(Field f) {
        StringBuilder type = new StringBuilder();
        switch (f.type()) {
            case BIT:
                if (f.precision() > 1 && !f.isSigned()) {
                    type.append('U');
                }
                if (f.precision() > 64) {
                    type.append(TYPE_HUGEINT);
                } else if (f.precision() > 32) {
                    type.append(TYPE_BIGINT);
                } else if (f.precision() > 16) {
                    type.append(TYPE_INTEGER);
                } else if (f.precision() > 8) {
                    type.append(TYPE_SMALLINT);
                } else if (f.precision() > 1) {
                    type.append(TYPE_TINYINT);
                } else {
                    type.append(TYPE_BOOLEAN);
                }
                break;
            case BOOLEAN:
                type.append(TYPE_BOOLEAN);
                break;
            case TINYINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append(TYPE_TINYINT);
                break;
            case SMALLINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append(TYPE_SMALLINT);
                break;
            case INTEGER:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append(TYPE_INTEGER);
                break;
            case BIGINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append(TYPE_BIGINT);
                break;
            case REAL:
            case FLOAT:
                type.append(TYPE_REAL);
                break;
            case DOUBLE:
                type.append(TYPE_DOUBLE);
                break;
            case NUMERIC:
                if (!f.isSigned()) {
                    type.append('U');
                }
                if (f.precision() > 20) {
                    type.append(TYPE_HUGEINT);
                } else if (f.precision() > 10) {
                    type.append(TYPE_BIGINT);
                } else if (f.precision() > 5) {
                    type.append(TYPE_INTEGER);
                } else if (f.precision() > 3) {
                    type.append(TYPE_SMALLINT);
                } else {
                    type.append(TYPE_TINYINT);
                }
                break;
            case DECIMAL:
                type.append(TYPE_DECIMAL).append('(').append(f.precision()).append(',').append(f.scale()).append(')');
                break;
            case DATE:
                type.append(TYPE_DATE);
                break;
            case TIME:
                type.append(TYPE_TIME);
                break;
            case TIME_WITH_TIMEZONE:
                type.append(TYPE_TIMETZ);
                break;
            case TIMESTAMP:
                type.append(TYPE_TIMESTAMP);
                if (f.scale() > 6) {
                    type.append(SUFFIX_NS); // nanosecond
                } else if (f.scale() > 0) {
                    type.append(SUFFIX_MS); // millisecond
                } else {
                    type.append(SUFFIX_SEC); // second
                }
                break;
            case TIMESTAMP_WITH_TIMEZONE:
                type.append(TYPE_TIMESTAMPTZ);
                break;
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                type.append(TYPE_VARCHAR);
                if (f.precision() > 0) {
                    // The maximum length n has no effect and is only provided for compatibility.
                    type.append('(').append(f.precision()).append(')');
                }
                break;
            case BINARY:
            case BLOB:
            case VARBINARY:
                type.append(TYPE_BLOB);
                break;
            // case ARRAY:
            // case OTHER:
            // case NULL:
            default:
                log.warn("Not sure how to map JDBC type [%s], use VARCHAR instead", f.type());
                type.append(TYPE_VARCHAR);
                break;
        }
        if (!f.isNullable()) {
            type.append(" NOT NULL");
        }
        return type.toString();
    }

    @Override
    public String toRemoteTable(String url, Format format, Compression compress, Result<?> result) {
        if (format == null || result == null) {
            return ResultMapper.super.toRemoteTable(url, format, compress, result);
        }

        final String struct = toStructDefinition(result.fields().toArray(new Field[0]));
        StringBuilder builder = new StringBuilder();
        switch (format) {
            case JSONL:
            case NDJSON:
                builder.append(FUNC_READ_JSON).append(url).append("',format='newline_delimited'");
                break;
            case PARQUET:
                return builder.append(FUNC_READ_PARQUET).append(url).append("')").toString();
            case TSV:
                builder.append(FUNC_READ_CSV).append(url).append("',delim='\\t',header=true");
                break;
            case CSV:
            default:
                builder.append(FUNC_READ_CSV).append(url).append("',header=true");
                break;
        }

        builder.append(",columns=").append(struct);
        if (compress != null && compress != Compression.NONE) {
            builder.append(",compression='").append(compress.encoding()).append('\'');
        }
        return builder.append(')').toString();
    }
}
