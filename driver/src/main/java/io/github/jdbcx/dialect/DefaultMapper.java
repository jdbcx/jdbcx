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
package io.github.jdbcx.dialect;

import io.github.jdbcx.Field;
import io.github.jdbcx.Logger;
import io.github.jdbcx.LoggerFactory;
import io.github.jdbcx.ResultMapper;

public class DefaultMapper implements ResultMapper {
    private static final Logger log = LoggerFactory.getLogger(DefaultMapper.class);

    static final String TYPE_VARCHAR = "VARCHAR";

    @Override
    public String toColumnType(Field f) {
        StringBuilder type = new StringBuilder();
        switch (f.type()) {
            case BIT:
                if (f.precision() > 64) {
                    type.append("DECIMAL(").append(f.precision() / 8 + f.precision() % 8 > 0 ? 1 : 0)
                            .append(", 0)");
                } else if (f.precision() > 32) {
                    type.append("BIGINT");
                } else if (f.precision() > 16) {
                    type.append("INT");
                } else if (f.precision() > 8) {
                    type.append("SMALLINT");
                } else {
                    type.append("TINYINT");
                }
                break;
            case CHAR:
            case NCHAR:
                if (f.precision() < 1) {
                    type.append(TYPE_VARCHAR);
                } else {
                    type.append("CHAR(").append(f.precision()).append(')');
                }
                break;
            case BOOLEAN:
            case TINYINT:
                type.append("TINYINT");
                break;
            case SMALLINT:
                type.append("SMALLINT");
                break;
            case INTEGER:
                type.append("INTEGER");
                break;
            case BIGINT:
                type.append("BIGINT");
                break;
            case REAL:
            case FLOAT:
                type.append("FLOAT");
                break;
            case DOUBLE:
                type.append("DOUBLE");
                break;
            case NUMERIC:
            case DECIMAL:
                type.append("DECIMAL(").append(f.precision()).append(',').append(f.scale()).append(')');
                break;
            case DATE:
                type.append("DATE");
                break;
            case TIME:
                type.append("TIME");
                break;
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                type.append("DATETIME");
                break;
            // case BINARY:
            // case VARBINARY:
            // case LONGVARBINARY:
            // case BLOB:
            //     type.append("BLOB");
            //     break;
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case CLOB:
                type.append(TYPE_VARCHAR);
                if (f.precision() > 0) {
                    type.append('(').append(f.precision()).append(')');
                }
                break;
            default:
                log.warn("Not sure how to map JDBC type [%s], use VARCHAR instead", f.type());
                type.append(TYPE_VARCHAR);
                break;
        }
        if (f.isNullable()) {
            type.append(" NULL");
        } else {
            type.append(" NOT NULL");
        }
        return type.toString();
    }

    protected DefaultMapper() {
    }
}
