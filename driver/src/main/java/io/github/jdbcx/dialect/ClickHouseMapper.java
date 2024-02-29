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

import java.util.ArrayList;
import java.util.List;

import io.github.jdbcx.Compression;
import io.github.jdbcx.Constants;
import io.github.jdbcx.Field;
import io.github.jdbcx.Format;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.Result;
import io.github.jdbcx.ResultMapper;
import io.github.jdbcx.Utils;

public class ClickHouseMapper implements ResultMapper {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseMapper.class);

    static final String FORMAT_CSV = "CSVWithNames";
    static final String FORMAT_TSV = "TSVWithNames";
    static final String FORMAT_JSONEACHLINE = "JSONEachLine";
    static final String FORMAT_LINEASSTRING = "LineAsString";

    static final String TYPE_NULLABLE = "Nullable";

    static final String TYPE_BOOL = "Boolean";
    static final String TYPE_INT8 = "Int8";
    static final String TYPE_INT16 = "Int16";
    static final String TYPE_INT32 = "Int32";
    static final String TYPE_INT64 = "Int64";
    static final String TYPE_INT128 = "Int128";
    static final String TYPE_INT256 = "Int256";
    static final String TYPE_FLOAT32 = "Float32";
    static final String TYPE_FLOAT64 = "Float64";
    static final String TYPE_DECIMAL = "Decimal";
    static final String TYPE_DATE = "Date";
    static final String TYPE_DATE32 = "Date32";
    static final String TYPE_TIME = "Time";
    static final String TYPE_DATETIME = "DateTime";
    static final String TYPE_DATETIME64 = "DateTime64";
    static final String TYPE_FIXEDSTRING = "FixedString";
    static final String TYPE_STRING = "String";

    static String toClickHouseDataFormat(Format f) {
        if (f == null) {
            return FORMAT_CSV;
        }

        final String format;
        switch (f) {
            case JSONL:
            case NDJSON:
                format = FORMAT_JSONEACHLINE;
                break;
            case TSV:
                format = FORMAT_TSV;
                break;
            case CSV:
            default:
                format = FORMAT_CSV;
                break;
        }
        return format;
    }

    @Override
    public String quoteIdentifier(String name) {
        return new StringBuilder().append('`').append(Utils.escape(name, '`', '\\')).append('`').toString();
    }

    @Override
    public String toColumnType(Field f) {
        StringBuilder type = new StringBuilder();
        if (f.isNullable()) {
            type.append(TYPE_NULLABLE).append('(');
        }
        switch (f.type()) {
            case BIT:
                if (f.precision() > 1 && !f.isSigned()) {
                    type.append('U');
                }
                if (f.precision() > 128) {
                    type.append(TYPE_INT256);
                } else if (f.precision() > 64) {
                    type.append(TYPE_INT128);
                } else if (f.precision() > 32) {
                    type.append(TYPE_INT64);
                } else if (f.precision() > 16) {
                    type.append(TYPE_INT32);
                } else if (f.precision() > 8) {
                    type.append(TYPE_INT16);
                } else if (f.precision() > 1) {
                    type.append(TYPE_INT8);
                } else {
                    type.append(TYPE_BOOL);
                }
                break;
            case CHAR:
            case NCHAR:
                if (f.precision() < 1) {
                    type.append(TYPE_STRING);
                } else {
                    type.append(TYPE_FIXEDSTRING).append('(').append(f.precision()).append(')');
                }
                break;
            case BOOLEAN:
                type.append(TYPE_BOOL);
                break;
            case TINYINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append(TYPE_INT8);
                break;
            case SMALLINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append(TYPE_INT16);
                break;
            case INTEGER:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append(TYPE_INT32);
                break;
            case BIGINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append(TYPE_INT64);
                break;
            case REAL:
            case FLOAT:
                type.append(TYPE_FLOAT32);
                break;
            case DOUBLE:
                type.append(TYPE_FLOAT64);
                break;
            case NUMERIC:
                if (!f.isSigned()) {
                    type.append('U');
                }
                if (f.precision() > 39) {
                    type.append(TYPE_INT256);
                } else if (f.precision() > 20) {
                    type.append(TYPE_INT128);
                } else if (f.precision() > 10) {
                    type.append(TYPE_INT64);
                } else if (f.precision() > 5) {
                    type.append(TYPE_INT32);
                } else if (f.precision() > 3) {
                    type.append(TYPE_INT16);
                } else {
                    type.append(TYPE_INT8);
                }
                break;
            case DECIMAL:
                type.append(TYPE_DECIMAL).append('(').append(f.precision()).append(',').append(f.scale()).append(')');
                break;
            case DATE:
                type.append(TYPE_DATE32);
                break;
            case TIME:
                type.append(TYPE_TIME);
                break;
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                type.append(TYPE_DATETIME64);
                if (f.scale() > 0) {
                    type.append('(')
                            .append(f.scale() <= Constants.MAX_TIME_SCALE ? f.scale() : Constants.MAX_TIME_SCALE)
                            .append(')');
                }
                break;
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                type.append(TYPE_STRING);
                break;
            // case ARRAY:
            // case OTHER:
            // case NULL:
            default:
                log.warn("Not sure how to map JDBC type [%s], use String instead", f.type());
                type.append(TYPE_STRING);
                break;
        }
        if (f.isNullable()) {
            type.append(')');
        }
        return type.toString();
    }

    @Override
    public String toRemoteTable(String url, Format format, Compression compress, Result<?> result) {
        StringBuilder builder = new StringBuilder().append("url('").append(url).append('\'');
        if (format == null && result == null && (compress == null || compress == Compression.NONE)) {
            return builder.append(',').append(FORMAT_LINEASSTRING).append(')').toString();
        }

        builder.append(',').append(toClickHouseDataFormat(format));
        if (result != null) {
            builder.append(",'")
                    .append(Utils.escape(toColumnDefinition(result.fields().toArray(new Field[0])), '\'', '\\'))
                    .append('\'');
        }
        List<String> parts = new ArrayList<>(2);
        if (format != null) {
            parts.add(new StringBuilder("'accept'='").append(format.mimeType()).append('\'').toString());
        }
        if (compress != null && compress != Compression.NONE) {
            parts.add(new StringBuilder("'accept-encoding'='").append(compress.encoding()).append('\'').toString());
        }
        if (parts.isEmpty()) {
            builder.append(')');
        } else {
            builder.append(",headers(").append(String.join(",", parts)).append("))");
        }
        return builder.toString();
    }
}