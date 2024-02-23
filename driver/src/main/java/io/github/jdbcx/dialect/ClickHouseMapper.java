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

public class ClickHouseMapper implements ResultMapper {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseMapper.class);

    static final String DEFAULT_FORMAT = "CSVWithNames";

    static String toClickHouseDataFormat(Format f) {
        if (f == null) {
            return DEFAULT_FORMAT;
        }

        final String format;
        switch (f) {
            case JSONL:
            case NDJSON:
                format = "JSONEachLine";
                break;
            case TSV:
                format = "TSVWithNames";
                break;
            case CSV:
            default:
                format = DEFAULT_FORMAT;
                break;
        }
        return format;
    }

    @Override
    public String toColumnType(Field f) {
        StringBuilder type = new StringBuilder();
        if (f.isNullable()) {
            type.append("Nullable(");
        }
        switch (f.type()) {
            case BIT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                if (f.precision() > 128) {
                    type.append("Int256");
                } else if (f.precision() > 64) {
                    type.append("Int128");
                } else if (f.precision() > 32) {
                    type.append("Int64");
                } else if (f.precision() > 16) {
                    type.append("Int32");
                } else if (f.precision() > 8) {
                    type.append("Int16");
                } else if (f.precision() > 1) {
                    type.append("Int8");
                } else {
                    type.setLength(0);
                    type.append("Boolean");
                }
                break;
            case CHAR:
            case NCHAR:
                type.append("FixedString(").append(f.precision()).append(')');
                break;
            case BOOLEAN:
                type.append("Boolean");
                break;
            case TINYINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append("Int8");
                break;
            case SMALLINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append("Int16");
                break;
            case INTEGER:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append("Int32");
                break;
            case BIGINT:
                if (!f.isSigned()) {
                    type.append('U');
                }
                type.append("Int64");
                break;
            case REAL:
            case FLOAT:
                type.append("Float32");
                break;
            case DOUBLE:
                type.append("Float64");
                break;
            case NUMERIC:
                if (!f.isSigned()) {
                    type.append('U');
                }
                if (f.precision() > 39) {
                    type.append("Int256");
                } else if (f.precision() > 20) {
                    type.append("Int128");
                } else if (f.precision() > 10) {
                    type.append("Int64");
                } else if (f.precision() > 5) {
                    type.append("Int32");
                } else if (f.precision() > 3) {
                    type.append("Int16");
                } else {
                    type.append("Int8");
                }
                break;
            case DECIMAL:
                type.append("Decimal(").append(f.precision()).append(',').append(f.scale()).append(')');
                break;
            case DATE:
                type.append("Date32");
                break;
            case TIME:
                type.append("Time");
                break;
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                type.append("DateTime64");
                break;
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                type.append("String");
                break;
            // case ARRAY:
            // case OTHER:
            // case NULL:
            default:
                log.warn("Not sure how to map JDBC type [%s], use String instead", f.type());
                type.append("String");
                break;
        }
        if (f.isNullable()) {
            type.append(')');
        }
        return type.toString();
    }

    @Override
    public String toRemoteTable(String url, Format format, Compression compress, Result<?> result) {
        StringBuilder builder = new StringBuilder();
        builder.append("url('").append(url).append("',").append(toClickHouseDataFormat(format)).append(",'")
                .append(toColumnDefinition(result.fields().toArray(new Field[0]))).append("',headers('accept'='")
                .append(format.mimeType()).append('\'');
        if (compress != null && compress != Compression.NONE) {
            builder.append(",'accept-encoding'='").append(compress.encoding()).append('\'');
        }
        return builder.append("))").toString();
    }
}
