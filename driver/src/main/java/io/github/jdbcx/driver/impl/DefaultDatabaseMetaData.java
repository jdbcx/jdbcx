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
package io.github.jdbcx.driver.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.github.jdbcx.Constants;
import io.github.jdbcx.DriverExtension;
import io.github.jdbcx.Field;
import io.github.jdbcx.Result;
import io.github.jdbcx.Version;
import io.github.jdbcx.executor.jdbc.CombinedResultSet;
import io.github.jdbcx.executor.jdbc.ReadOnlyResultSet;

final class DefaultDatabaseMetaData extends DefaultWrapper implements DatabaseMetaData {
    private final DefaultConnection conn;

    private final boolean readOnly;

    private final Version specVersion;

    DefaultDatabaseMetaData(DefaultConnection conn) {
        this.conn = conn;

        this.readOnly = conn.readOnly.get();
        this.specVersion = Version.of(DefaultDatabaseMetaData.class.getPackage().getSpecificationVersion());
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return conn.url;
    }

    @Override
    public String getUserName() throws SQLException {
        return System.getProperty("user.name", "unknown");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "JDBCX";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return Version.current().toCompactString();
    }

    @Override
    public String getDriverName() throws SQLException {
        return "JDBCX Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return Version.current().toCompactString();
    }

    @Override
    public int getDriverMajorVersion() {
        return Version.current().getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return Version.current().getMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return Constants.EMPTY_STRING;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return Constants.EMPTY_STRING;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return Constants.EMPTY_STRING;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return Constants.EMPTY_STRING;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return Constants.EMPTY_STRING;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED == level;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("PROCEDURE_CAT", JDBCType.VARCHAR),
                Field.of("PROCEDURE_SCHEM", JDBCType.VARCHAR), Field.of("RESERVED1", JDBCType.VARCHAR),
                Field.of("RESERVED2", JDBCType.VARCHAR), Field.of("RESERVED3", JDBCType.VARCHAR),
                Field.of("PROCEDURE_NAME", JDBCType.VARCHAR, false), Field.of("REMARKS", JDBCType.VARCHAR, false),
                Field.of("PROCEDURE_TYPE", JDBCType.SMALLINT, false),
                Field.of("SPECIFIC_NAME", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("PROCEDURE_CAT", JDBCType.VARCHAR),
                Field.of("PROCEDURE_SCHEM", JDBCType.VARCHAR), Field.of("PROCEDURE_NAME", JDBCType.VARCHAR, false),
                Field.of("COLUMN_NAME", JDBCType.VARCHAR, false), Field.of("COLUMN_TYPE", JDBCType.SMALLINT, false),
                Field.of("DATA_TYPE", JDBCType.SMALLINT, false), Field.of("TYPE_NAME", JDBCType.VARCHAR, false),
                Field.of("PRECISION", JDBCType.INTEGER, false), Field.of("LENGTH", JDBCType.INTEGER, false),
                Field.of("SCALE", JDBCType.SMALLINT, false), Field.of("RADIX", JDBCType.SMALLINT, false),
                Field.of("NULLABLE", JDBCType.SMALLINT, false), Field.of("REMARKS", JDBCType.VARCHAR, false),
                Field.of("COLUMN_DEF", JDBCType.VARCHAR), Field.of("SQL_DATA_TYPE", JDBCType.INTEGER, false),
                Field.of("SQL_DATETIME_SUB", JDBCType.INTEGER, false),
                Field.of("CHAR_OCTET_LENGTH", JDBCType.INTEGER, false),
                Field.of("ORDINAL_POSITION", JDBCType.INTEGER, false), Field.of("IS_NULLABLE", JDBCType.VARCHAR, false),
                Field.of("SPECIFIC_NAME", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        List<Field> fields = Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("TABLE_TYPE", JDBCType.VARCHAR, false), Field.of("REMARKS", JDBCType.VARCHAR),
                Field.of("TYPE_CAT", JDBCType.VARCHAR), Field.of("TYPE_SCHEM", JDBCType.VARCHAR),
                Field.of("TYPE_NAME", JDBCType.VARCHAR), Field.of("SELF_REFERENCING_COL_NAME", JDBCType.VARCHAR),
                Field.of("REF_GENERATION", JDBCType.VARCHAR));
        final List<DriverExtension> exts = conn.manager.getMatchedExtensions(catalog);

        int len = exts.size();
        ResultSet[] arr = new ResultSet[len + 1];
        arr[0] = new ReadOnlyResultSet(null, Result.of(fields));
        for (int i = 0; i < len; i++) {
            DriverExtension ext = exts.get(i);
            arr[i + 1] = ext.getTables(schemaPattern, tableNamePattern, types, conn.manager.extractProperties(ext));
        }
        return new CombinedResultSet(arr);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        List<DriverExtension> exts = conn.manager.getExtensions();
        int size = exts.size();
        String[] names = new String[size];
        for (int i = 0; i < size; i++) {
            names[i] = exts.get(i).getName();
        }

        return new ReadOnlyResultSet(null,
                Result.of(Collections.singletonList(Field.of("TABLE_CAT", JDBCType.VARCHAR, false)), names));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("TABLE_TYPE", JDBCType.VARCHAR, false)),
                new String[] { "TABLE", "VIEW" }));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("COLUMN_NAME", JDBCType.VARCHAR, false), Field.of("DATA_TYPE", JDBCType.INTEGER, false),
                Field.of("TYPE_NAME", JDBCType.VARCHAR, false), Field.of("COLUMN_SIZE", JDBCType.INTEGER, false),
                Field.of("BUFFER_LENGTH", JDBCType.INTEGER, false), Field.of("DECIMAL_DIGITS", JDBCType.INTEGER, false),
                Field.of("NUM_PREC_RADIX", JDBCType.INTEGER, false), Field.of("NULLABLE", JDBCType.INTEGER, false),
                Field.of("REMARKS", JDBCType.VARCHAR), Field.of("COLUMN_DEF", JDBCType.VARCHAR),
                Field.of("SQL_DATA_TYPE", JDBCType.INTEGER, false),
                Field.of("SQL_DATETIME_SUB", JDBCType.INTEGER, false),
                Field.of("CHAR_OCTET_LENGTH", JDBCType.INTEGER, false),
                Field.of("ORDINAL_POSITION", JDBCType.INTEGER, false),
                Field.of("IS_NULLABLE", JDBCType.VARCHAR, false), Field.of("SCOPE_CATALOG", JDBCType.VARCHAR),
                Field.of("SCOPE_SCHEMA", JDBCType.VARCHAR), Field.of("SCOPE_TABLE", JDBCType.VARCHAR),
                Field.of("SOURCE_DATA_TYPE", JDBCType.SMALLINT), Field.of("IS_AUTOINCREMENT", JDBCType.VARCHAR, false),
                Field.of("IS_GENERATEDCOLUMN", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("COLUMN_NAME", JDBCType.VARCHAR, false), Field.of("GRANTOR", JDBCType.VARCHAR),
                Field.of("GRANTEE", JDBCType.VARCHAR, false), Field.of("PRIVILEGE", JDBCType.VARCHAR, false),
                Field.of("IS_GRANTABLE", JDBCType.VARCHAR))));
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("GRANTOR", JDBCType.VARCHAR), Field.of("GRANTEE", JDBCType.VARCHAR, false),
                Field.of("PRIVILEGE", JDBCType.VARCHAR, false), Field.of("IS_GRANTABLE", JDBCType.VARCHAR))));
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return getVersionColumns(catalog, schema, table);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("SCOPE", JDBCType.SMALLINT, false),
                Field.of("COLUMN_NAME", JDBCType.VARCHAR, false), Field.of("DATA_TYPE", JDBCType.INTEGER, false),
                Field.of("TYPE_NAME", JDBCType.VARCHAR, false), Field.of("COLUMN_SIZE", JDBCType.INTEGER, false),
                Field.of("BUFFER_LENGTH", JDBCType.INTEGER, false),
                Field.of("DECIMAL_DIGITS", JDBCType.SMALLINT, false),
                Field.of("PSEUDO_COLUMN", JDBCType.SMALLINT, false))));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("COLUMN_NAME", JDBCType.VARCHAR, false), Field.of("KEY_SEQ", JDBCType.SMALLINT, false),
                Field.of("PK_NAME", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("PKTABLE_CAT", JDBCType.VARCHAR),
                Field.of("PKTABLE_SCHEM", JDBCType.VARCHAR), Field.of("PKTABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("PKCOLUMN_NAME", JDBCType.VARCHAR, false), Field.of("FKTABLE_CAT", JDBCType.VARCHAR),
                Field.of("FKTABLE_SCHEM", JDBCType.VARCHAR), Field.of("FKTABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("FKCOLUMN_NAME", JDBCType.VARCHAR, false), Field.of("KEY_SEQ", JDBCType.SMALLINT, false),
                Field.of("UPDATE_RULE", JDBCType.SMALLINT, false), Field.of("DELETE_RULE", JDBCType.SMALLINT, false),
                Field.of("FK_NAME", JDBCType.VARCHAR), Field.of("PK_NAME", JDBCType.VARCHAR),
                Field.of("DEFERRABILITY", JDBCType.SMALLINT, false))));
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getImportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("PKTABLE_CAT", JDBCType.VARCHAR),
                Field.of("PKTABLE_SCHEM", JDBCType.VARCHAR), Field.of("PKTABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("PKCOLUMN_NAME", JDBCType.VARCHAR, false), Field.of("FKTABLE_CAT", JDBCType.VARCHAR),
                Field.of("FKTABLE_SCHEM", JDBCType.VARCHAR), Field.of("FKTABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("FKCOLUMN_NAME", JDBCType.VARCHAR, false), Field.of("KEY_SEQ", JDBCType.SMALLINT, false),
                Field.of("UPDATE_RULE", JDBCType.SMALLINT, false), Field.of("DELETE_RULE", JDBCType.SMALLINT, false),
                Field.of("FK_NAME", JDBCType.VARCHAR), Field.of("PK_NAME", JDBCType.VARCHAR),
                Field.of("DEFERRABILITY", JDBCType.SMALLINT, false))));
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("TYPE_NAME", JDBCType.VARCHAR, false),
                Field.of("DATA_TYPE", JDBCType.INTEGER, false), Field.of("PRECISION", JDBCType.INTEGER, false),
                Field.of("LITERAL_PREFIX", JDBCType.VARCHAR), Field.of("LITERAL_SUFFIX", JDBCType.VARCHAR),
                Field.of("CREATE_PARAMS", JDBCType.VARCHAR), Field.of("NULLABLE", JDBCType.SMALLINT, false),
                Field.of("CASE_SENSITIVE", JDBCType.BOOLEAN, false), Field.of("SEARCHABLE", JDBCType.SMALLINT, false),
                Field.of("UNSIGNED_ATTRIBUTE", JDBCType.BOOLEAN, false),
                Field.of("FIXED_PREC_SCALE", JDBCType.BOOLEAN, false),
                Field.of("AUTO_INCREMENT", JDBCType.BOOLEAN, false), Field.of("LOCAL_TYPE_NAME", JDBCType.VARCHAR),
                Field.of("MINIMUM_SCALE", JDBCType.SMALLINT, false),
                Field.of("MAXIMUM_SCALE", JDBCType.SMALLINT, false),
                Field.of("SQL_DATA_TYPE", JDBCType.INTEGER, false),
                Field.of("SQL_DATETIME_SUB", JDBCType.INTEGER, false),
                Field.of("NUM_PREC_RADIX", JDBCType.INTEGER, false))));
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("NON_UNIQUE", JDBCType.BOOLEAN, false), Field.of("INDEX_QUALIFIER", JDBCType.VARCHAR),
                Field.of("INDEX_NAME", JDBCType.VARCHAR), Field.of("TYPE", JDBCType.SMALLINT, false),
                Field.of("ORDINAL_POSITION", JDBCType.SMALLINT, false), Field.of("COLUMN_NAME", JDBCType.VARCHAR),
                Field.of("ASC_OR_DESC", JDBCType.VARCHAR), Field.of("CARDINALITY", JDBCType.BIGINT, false),
                Field.of("PAGES", JDBCType.BIGINT, false), Field.of("FILTER_CONDITION", JDBCType.VARCHAR))));
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return new ReadOnlyResultSet(null,
                Result.of(Arrays.asList(Field.of("TYPE_CAT", JDBCType.VARCHAR),
                        Field.of("TYPE_SCHEM", JDBCType.VARCHAR), Field.of("TYPE_NAME", JDBCType.VARCHAR, false),
                        Field.of("CLASS_NAME", JDBCType.VARCHAR, false), Field.of("DATA_TYPE", JDBCType.INTEGER, false),
                        Field.of("REMARKS", JDBCType.VARCHAR, false),
                        Field.of("BASE_TYPE", JDBCType.SMALLINT, false))));
    }

    @Override
    public DefaultConnection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new ReadOnlyResultSet(null,
                Result.of(Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                        Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TYPE_NAME", JDBCType.VARCHAR, false),
                        Field.of("SUPERTYPE_CAT", JDBCType.VARCHAR), Field.of("SUPERTYPE_SCHEM", JDBCType.VARCHAR),
                        Field.of("SUPERTYPE_NAME", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new ReadOnlyResultSet(null,
                Result.of(Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                        Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TABLE_NAME", JDBCType.VARCHAR, false),
                        Field.of("SUPERTABLE_NAME", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return new ReadOnlyResultSet(null,
                Result.of(Arrays.asList(Field.of("TYPE_CAT", JDBCType.VARCHAR),
                        Field.of("TYPE_SCHEM", JDBCType.VARCHAR), Field.of("TYPE_NAME", JDBCType.VARCHAR, false),
                        Field.of("ATTR_NAME", JDBCType.VARCHAR, false), Field.of("DATA_TYPE", JDBCType.INTEGER, false),
                        Field.of("ATTR_TYPE_NAME", JDBCType.VARCHAR, false),
                        Field.of("ATTR_SIZE", JDBCType.INTEGER, false),
                        Field.of("DECIMAL_DIGITS", JDBCType.INTEGER, false),
                        Field.of("NUM_PREC_RADIX", JDBCType.INTEGER, false),
                        Field.of("NULLABLE", JDBCType.INTEGER, false), Field.of("REMARKS", JDBCType.VARCHAR),
                        Field.of("ATTR_DEF", JDBCType.VARCHAR), Field.of("SQL_DATA_TYPE", JDBCType.INTEGER, false),
                        Field.of("SQL_DATETIME_SUB", JDBCType.INTEGER, false),
                        Field.of("CHAR_OCTET_LENGTH", JDBCType.INTEGER, false),
                        Field.of("CHAR_OCTET_LENGTH", JDBCType.INTEGER, false),
                        Field.of("ORDINAL_POSITION", JDBCType.INTEGER, false),
                        Field.of("IS_NULLABLE", JDBCType.VARCHAR, false),
                        Field.of("SCOPE_CATALOG", JDBCType.VARCHAR, false),
                        Field.of("SCOPE_SCHEMA", JDBCType.VARCHAR, false),
                        Field.of("SCOPE_TABLE", JDBCType.VARCHAR, false),
                        Field.of("SOURCE_DATA_TYPE", JDBCType.SMALLINT, false))));
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return Version.current().getMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return Version.current().getMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return specVersion.getMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return specVersion.getMinorVersion();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<Field> fields = Arrays.asList(Field.of("TABLE_SCHEM", JDBCType.VARCHAR, false),
                Field.of("TABLE_CATALOG", JDBCType.VARCHAR));
        final List<DriverExtension> exts = conn.manager.getMatchedExtensions(catalog);
        int len = exts.size();
        ResultSet[] arr = new ResultSet[len];
        for (int i = 0; i < len; i++) {
            DriverExtension ext = exts.get(i);
            List<String> schemas = ext.getSchemas(schemaPattern, conn.manager.extractProperties(ext));
            if (schemas == null || schemas.isEmpty()) {
                arr[i] = null;
            } else {
                int size = schemas.size();
                String extName = ext.getName();
                String[][] ss = new String[size][];
                for (int j = 0; j < size; j++) {
                    ss[j] = new String[] { schemas.get(j), extName };
                }
                arr[i] = new ReadOnlyResultSet(null, Result.of(fields, ss));
            }
        }
        return new CombinedResultSet(arr);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("NAME", JDBCType.VARCHAR, false),
                Field.of("MAX_LEN", JDBCType.INTEGER, false), Field.of("DEFAULT_VALUE", JDBCType.VARCHAR, false),
                Field.of("DESCRIPTION", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("FUNCTION_CAT", JDBCType.VARCHAR),
                Field.of("FUNCTION_SCHEM", JDBCType.VARCHAR), Field.of("FUNCTION_NAME", JDBCType.VARCHAR, false),
                Field.of("REMARKS", JDBCType.VARCHAR, false), Field.of("FUNCTION_TYPE", JDBCType.SMALLINT, false),
                Field.of("SPECIFIC_NAME", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("FUNCTION_CAT", JDBCType.VARCHAR),
                Field.of("FUNCTION_SCHEM", JDBCType.VARCHAR), Field.of("FUNCTION_NAME", JDBCType.VARCHAR, false),
                Field.of("COLUMN_NAME", JDBCType.VARCHAR, false), Field.of("COLUMN_TYPE", JDBCType.SMALLINT, false),
                Field.of("DATA_TYPE", JDBCType.SMALLINT, false), Field.of("TYPE_NAME", JDBCType.VARCHAR, false),
                Field.of("PRECISION", JDBCType.INTEGER, false), Field.of("LENGTH", JDBCType.INTEGER, false),
                Field.of("SCALE", JDBCType.SMALLINT, false), Field.of("RADIX", JDBCType.SMALLINT, false),
                Field.of("NULLABLE", JDBCType.SMALLINT, false), Field.of("REMARKS", JDBCType.VARCHAR, false),
                Field.of("CHAR_OCTET_LENGTH", JDBCType.INTEGER, false),
                Field.of("ORDINAL_POSITION", JDBCType.INTEGER, false), Field.of("IS_NULLABLE", JDBCType.VARCHAR, false),
                Field.of("SPECIFIC_NAME", JDBCType.VARCHAR, false))));
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return new ReadOnlyResultSet(null, Result.of(Arrays.asList(Field.of("TABLE_CAT", JDBCType.VARCHAR),
                Field.of("TABLE_SCHEM", JDBCType.VARCHAR), Field.of("TABLE_NAME", JDBCType.VARCHAR, false),
                Field.of("COLUMN_NAME", JDBCType.VARCHAR, false), Field.of("DATA_TYPE", JDBCType.INTEGER, false),
                Field.of("COLUMN_SIZE", JDBCType.INTEGER, false), Field.of("DECIMAL_DIGITS", JDBCType.INTEGER, false),
                Field.of("NUM_PREC_RADIX", JDBCType.INTEGER, false), Field.of("COLUMN_USAGE", JDBCType.SMALLINT, false),
                Field.of("REMARKS", JDBCType.VARCHAR, false), Field.of("CHAR_OCTET_LENGTH", JDBCType.INTEGER, false),
                Field.of("IS_NULLABLE", JDBCType.VARCHAR, false))));
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
