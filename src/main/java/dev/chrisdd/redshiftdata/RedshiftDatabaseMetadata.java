package dev.chrisdd.redshiftdata;

import dev.chrisdd.redshiftdata.ripped.EscapedFunctions;
import software.amazon.awssdk.services.redshiftdata.model.ColumnMetadata;
import software.amazon.awssdk.services.redshiftdata.model.DescribeTableResponse;
import software.amazon.awssdk.services.redshiftdata.model.Field;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultResponse;
import software.amazon.awssdk.services.redshiftdata.paginators.ListDatabasesIterable;
import software.amazon.awssdk.services.redshiftdata.paginators.ListSchemasIterable;
import software.amazon.awssdk.services.redshiftdata.paginators.ListTablesIterable;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

class RedshiftDatabaseMetadata implements DatabaseMetaData {

    private final RedshiftConnection conn;
    private String username;

    public RedshiftDatabaseMetadata(RedshiftConnection conn) {
        this.conn = conn;
    }


    @Override
    public String getURL() throws SQLException {
        return this.conn.getConfig().getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        if (this.username != null) {
            return this.username;
        }
        ResultSet rs = this.conn.createStatement().executeQuery("select current_user()");
        rs.next();
        this.username = rs.getString(1);
        return this.username;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Redshift";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        ResultSet rs = this.conn.createStatement().executeQuery("select version()");
        rs.next();
        return rs.getString(1);
    }

    @Override
    public String getDriverName() throws SQLException {
        return "redshiftdata-jdbc";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return String.format("%d.%d", getDriverMajorVersion(), getDriverMinorVersion());
    }

    @Override
    public int getDriverMajorVersion() {
        return RedshiftDriver.MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return RedshiftDriver.MINOR_VERSION;
    }


    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        // Static list from PG8.2 src/backend/parser/keywords.c with SQL:2003 excluded.
        String keywords = "abort,access,aggregate,also,analyse,analyze,backward,bit,cache,checkpoint,class,"
                + "cluster,comment,concurrently,connection,conversion,copy,csv,database,delimiter,"
                + "delimiters,disable,do,enable,encoding,encrypted,exclusive,explain,force,forward,freeze,"
                + "greatest,handler,header,if,ilike,immutable,implicit,index,indexes,inherit,inherits,"
                + "instead,isnull,least,limit,listen,load,location,lock,mode,move,nothing,notify,notnull,"
                + "nowait,off,offset,oids,operator,owned,owner,password,prepared,procedural,quote,reassign,"
                + "recheck,reindex,rename,replace,reset,restrict,returning,rule,setof,share,show,stable,"
                + "statistics,stdin,stdout,storage,strict,sysid,tablespace,temp,template,truncate,trusted,"
                + "unencrypted,unlisten,until,vacuum,valid,validator,verbose,volatile";
        return keywords;
    }

    public String getNumericFunctions() throws SQLException {
        return EscapedFunctions.ABS + ',' + EscapedFunctions.ACOS + ',' + EscapedFunctions.ASIN + ','
                + EscapedFunctions.ATAN + ',' + EscapedFunctions.ATAN2 + ',' + EscapedFunctions.CEILING
                + ',' + EscapedFunctions.COS + ',' + EscapedFunctions.COT + ',' + EscapedFunctions.DEGREES
                + ',' + EscapedFunctions.EXP + ',' + EscapedFunctions.FLOOR + ',' + EscapedFunctions.LOG
                + ',' + EscapedFunctions.LOG10 + ',' + EscapedFunctions.MOD + ',' + EscapedFunctions.PI
                + ',' + EscapedFunctions.POWER + ',' + EscapedFunctions.RADIANS + ',' + EscapedFunctions.RANDOM + ','
                + EscapedFunctions.ROUND + ',' + EscapedFunctions.SIGN + ',' + EscapedFunctions.SIN + ','
                + EscapedFunctions.SQRT + ',' + EscapedFunctions.TAN + ',' + EscapedFunctions.TRUNCATE;

    }

    public String getStringFunctions() throws SQLException {
        String funcs = EscapedFunctions.ASCII + ',' + EscapedFunctions.CHAR + ','
                + EscapedFunctions.CHAR_LENGTH + ',' + EscapedFunctions.CHARACTER_LENGTH + ','
                + EscapedFunctions.CONCAT + ',' + EscapedFunctions.LCASE + ',' + EscapedFunctions.LEFT + ','
                + EscapedFunctions.LENGTH + ',' + EscapedFunctions.LTRIM + ','
                + EscapedFunctions.OCTET_LENGTH + ',' + EscapedFunctions.POSITION + ','
                + EscapedFunctions.REPEAT + ','
                + EscapedFunctions.RIGHT + ',' + EscapedFunctions.RTRIM + ',' + EscapedFunctions.SPACE + ','
                + EscapedFunctions.SUBSTRING + ',' + EscapedFunctions.UCASE;

        // Currently these don't work correctly with parameterized
        // arguments, so leave them out. They reorder the arguments
        // when rewriting the query, but no translation layer is provided,
        // so a setObject(N, obj) will not go to the correct parameter.
        // ','+EscapedFunctions.INSERT+','+EscapedFunctions.LOCATE+
        // ','+EscapedFunctions.RIGHT+

        funcs += ',' + EscapedFunctions.REPLACE;

        return funcs;
    }

    public String getSystemFunctions() throws SQLException {
        return EscapedFunctions.DATABASE + ',' + EscapedFunctions.IFNULL + ',' + EscapedFunctions.USER;
    }

    public String getTimeDateFunctions() throws SQLException {
        String timeDateFuncs = EscapedFunctions.CURDATE + ',' + EscapedFunctions.CURTIME + ','
                + EscapedFunctions.DAYNAME + ',' + EscapedFunctions.DAYOFMONTH + ','
                + EscapedFunctions.DAYOFWEEK + ',' + EscapedFunctions.DAYOFYEAR + ','
                + EscapedFunctions.HOUR + ',' + EscapedFunctions.MINUTE + ',' + EscapedFunctions.MONTH + ','
                + EscapedFunctions.MONTHNAME + ',' + EscapedFunctions.NOW + ',' + EscapedFunctions.QUARTER
                + ',' + EscapedFunctions.SECOND + ',' + EscapedFunctions.WEEK + ',' + EscapedFunctions.YEAR;

        timeDateFuncs += ',' + EscapedFunctions.TIMESTAMPADD;

        // +','+EscapedFunctions.TIMESTAMPDIFF;

        return timeDateFuncs;
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
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
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
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        ListTablesIterable tables = this.conn.listTables(schemaPattern, tableNamePattern);
        ColumnMetadata[] columns = new ColumnMetadata[]{
                ColumnMetadata.builder()
                        .name("table_cat")
                        .typeName("varchar")
                        .label("table_cat")
                        .length(0)
                        .precision(124)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("table_schem")
                        .label("table_schem")
                        .typeName("varchar")
                        .length(0)
                        .precision(124)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("table_name")
                        .label("table_name")
                        .typeName("varchar")
                        .length(0)
                        .precision(128)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("table_type")
                        .label("table_type")
                        .typeName("varchar")
                        .length(0)
                        .precision(128)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("remarks")
                        .label("remarks")
                        .typeName("varchar")
                        .length(0)
                        .precision(65535)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("type_cat")
                        .label("type_cat")
                        .typeName("varchar")
                        .length(0)
                        .precision(65535)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("type_schem")
                        .label("type_schem")
                        .typeName("varchar")
                        .length(0)
                        .precision(65535)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("type_name")
                        .label("type_name")
                        .typeName("varchar")
                        .length(0)
                        .precision(65535)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("self_referencing_col_name")
                        .label("self_referencing_col_name")
                        .typeName("varchar")
                        .length(0)
                        .precision(65535)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("ref_generation")
                        .label("ref_generation")
                        .typeName("varchar")
                        .length(0)
                        .precision(65535)
                        .scale(0)
                        .isCaseSensitive(true)
                        .isCurrency(false)
                        .schemaName("")
                        .tableName("")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
        };
        List<List<Field>> records = tables.stream()
                .flatMap(resp -> resp.tables().stream())
                .map(d -> Arrays.asList(
                        Field.builder().stringValue(this.conn.getConfig().getDatabase()).build(),
                        Field.builder().stringValue(d.schema()).build(),
                        Field.builder().stringValue(d.name()).build(),
                        Field.builder().stringValue(d.type()).build(),
                        Field.builder().isNull(true).build(),
                        Field.builder().stringValue("").build(),
                        Field.builder().stringValue("").build(),
                        Field.builder().stringValue("").build(),
                        Field.builder().stringValue("").build(),
                        Field.builder().stringValue("").build()
                ))
                .sorted(Comparator.comparing(f -> f.get(3).stringValue() + "." + f.get(1).stringValue() + "." + f.get(2).stringValue()))
                .collect(Collectors.toList());
        return new RedshiftResultSet(new RedshiftStatement(conn), Collections.singletonList(
                GetStatementResultResponse.builder()
                        .columnMetadata(columns)
                        .records(records)
                        .totalNumRows((long) records.size())
                        .build()).iterator());
//         Basic query, without the join operation and subquery name appended to the end
//        StringBuilder tableQuery = new StringBuilder(2048);
//        tableQuery.append("SELECT * FROM (SELECT CAST(current_database() AS VARCHAR(124)) AS TABLE_CAT,"
//                + " table_schema AS TABLE_SCHEM,"
//                + " table_name AS TABLE_NAME,"
//                + " CAST("
//                + " CASE table_type"
//                + " WHEN 'BASE TABLE' THEN CASE"
//                + " WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM TABLE'"
//                + " WHEN table_schema = 'pg_toast' THEN 'SYSTEM TOAST TABLE'"
//                + " WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY TABLE'"
//                + " ELSE 'TABLE'"
//                + " END"
//                + " WHEN 'VIEW' THEN CASE"
//                + " WHEN table_schema = 'pg_catalog' OR table_schema = 'information_schema' THEN 'SYSTEM VIEW'"
//                + " WHEN table_schema = 'pg_toast' THEN NULL"
//                + " WHEN table_schema ~ '^pg_' AND table_schema != 'pg_toast' THEN 'TEMPORARY VIEW'"
//                + " ELSE 'VIEW'"
//                + " END"
//                + " WHEN 'EXTERNAL TABLE' THEN 'EXTERNAL TABLE'"
//                + " END"
//                + " AS VARCHAR(124)) AS TABLE_TYPE,"
//                + " REMARKS,"
//                + " '' as TYPE_CAT,"
//                + " '' as TYPE_SCHEM,"
//                + " '' as TYPE_NAME, "
//                + " '' AS SELF_REFERENCING_COL_NAME,"
//                + " '' AS REF_GENERATION "
//                + " FROM svv_tables)");
//
//        tableQuery.append( " WHERE true ");
//
////        String filterClause = getTableFilterClause(catalog, schemaPattern, tableNamePattern, types, NO_SCHEMA_UNIVERSAL_QUERY, true, null);
//        String orderby = " ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ";
//
////        tableQuery.append(filterClause);
//        tableQuery.append(orderby);
//
//        return this.conn.createStatement().executeQuery(tableQuery.toString());

    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return this.getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        ListDatabasesIterable databases = this.conn.listDatabase();
        ColumnMetadata[] columns = new ColumnMetadata[]{
                ColumnMetadata.builder()
                        .name("TABLE_CAT")
                        .typeName("varchar")
                        .length(124)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build()
        };
        List<List<Field>> records = databases.stream().flatMap(resp -> resp.databases().stream()).map(d -> Arrays.asList(Field.builder().stringValue(d).build())).collect(Collectors.toList());
        return new RedshiftResultSet(new RedshiftStatement(conn), Collections.singletonList(
                GetStatementResultResponse.builder()
                        .columnMetadata(columns)
                        .records(records)
                        .totalNumRows((long) records.size())
                        .build()).iterator());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        ColumnMetadata[] columns = new ColumnMetadata[]{
                ColumnMetadata.builder()
                        .name("TABLE_TYPE")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
        };
        List<List<Field>> records = Arrays.stream(
                        new String[]{"TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM"})
                .map(d -> Collections.singletonList(Field.builder().stringValue(d).build()))
                .collect(Collectors.toList());
        return new RedshiftResultSet(new RedshiftStatement(conn), Collections.singletonList(
                GetStatementResultResponse.builder()
                        .columnMetadata(columns)
                        .records(records)
                        .totalNumRows((long) records.size())
                        .build()).iterator());
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Iterator<DescribeTableResponse> iter = this.conn.describeTable(schemaPattern, tableNamePattern);
        List<List<Field>> records = new ArrayList<>();
        long ordinal = 0;
        while (iter.hasNext()) {
            DescribeTableResponse resp = iter.next();
            for (ColumnMetadata c : resp.columnList()) {

                records.add(Arrays.asList(
                                Field.fromStringValue(this.conn.getConfig().getDatabase()),
                                Field.fromStringValue(c.schemaName()),
                                Field.fromStringValue(c.tableName()),
                                Field.fromStringValue(c.name()),
                                Field.fromLongValue((long) RedshiftResultSetMetadata.mapRedshiftType(c.typeName().toLowerCase())),
                                Field.fromStringValue(c.typeName()),
                                Field.fromLongValue(c.precision().longValue()),
                                Field.fromStringValue(""),
                                Field.fromLongValue(c.scale().longValue()),
                                Field.fromLongValue(2L),
                                Field.fromLongValue(c.nullable().longValue()),
                                Field.fromIsNull(true),
                                Field.fromStringValue(c.columnDefault() == null ? "" : c.columnDefault()),
                                Field.fromIsNull(true),
                                Field.fromIsNull(true),
                                Field.fromLongValue(c.length().longValue()),
                                Field.fromLongValue(ordinal++),
                                Field.fromStringValue(c.nullable() == columnNullable ? "YES" : "NO"),
                                Field.fromIsNull(true),
                                Field.fromIsNull(true),
                                Field.fromIsNull(true),
                                Field.fromIsNull(true),
                                Field.fromStringValue("NO"),
                                Field.fromStringValue("NO")
                        )
                );
            }
        }


        ColumnMetadata[] columns = new ColumnMetadata[]{
                ColumnMetadata.builder()
                        .name("TABLE_CAT")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("TABLE_SCHEM")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("TABLE_NAME")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("COLUMN_NAME")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("DATA_TYPE")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("TYPE_NAME")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("COLUMN_SIZE")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("BUFFER_LENGTH")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("DECIMAL_DIGITS")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("NUM_PREC_RADIX")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("NULLABLE")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("REMARKS")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("COLUMN_DEF")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SQL_DATA_TYPE")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SQL_DATETIME_SUB")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("CHAR_OCTET_LENGTH")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("ORDINAL_POSITION")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("IS_NULLABLE")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SCOPE_CATALOG")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SCOPE_SCHEMA")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SCOPE_TABLE")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SOURCE_DATA_TYPE")
                        .typeName("integer")
                        .precision(32)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("IS_AUTOINCREMENT")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("IS_GENERATEDCOLUMN")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
        };
        return new RedshiftResultSet(new RedshiftStatement(conn), Collections.singletonList(
                GetStatementResultResponse.builder()
                        .columnMetadata(columns)
                        .records(records)
                        .totalNumRows((long) records.size())
                        .build()).iterator());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported column privileges");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported table privileges");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported row identifier");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported version columns");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported primary keys");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported imported keys");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported export keys");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported cross references");
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return RedshiftTypeInfo.getTypeInfo(new RedshiftStatement(this.conn));
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported index info");
    }


    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.conn;
    }


    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported super types");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported super tables");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported attributes");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported row id lifetime");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        ListSchemasIterable schemas = this.conn.listSchemas(schemaPattern);
        ColumnMetadata[] columns = new ColumnMetadata[]{
                ColumnMetadata.builder()
                        .name("TABLE_SCHEM")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("TABLE_CATALOG")
                        .typeName("varchar")
                        .length(128)
                        .nullable(ResultSetMetaData.columnNullable)
                        .build()
        };
        List<List<Field>> records = schemas.stream()
                .flatMap(resp -> resp.schemas().stream())
                .map(d -> Arrays.asList(
                        Field.builder().stringValue(d).build(),
                        Field.builder().stringValue("").build()))
                .collect(Collectors.toList());
        return new RedshiftResultSet(new RedshiftStatement(conn), Collections.singletonList(
                GetStatementResultResponse.builder()
                        .columnMetadata(columns)
                        .records(records)
                        .totalNumRows((long) records.size())
                        .build()).iterator());
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported getClientInfo");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported get functions");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported get function columns");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported pseudo columns");
    }


    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface))
            return iface.cast(this);
        throw new SQLException(String.format("%s is not a wrapper for %s", getClass().getName(), iface.getName()));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        /*
         * Currently LOB's aren't updateable at all, so it doesn't matter what we return. We don't throw
         * the notImplemented Exception because the 1.5 JDK's CachedRowSet calls this method regardless
         * of whether large objects are used.
         */
        return true;
    }

    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    public boolean supportsSavepoints() throws SQLException {
        return true;
    }

    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        // We don't support returning generated keys by column index,
        // but that should be a rarer case than the ones we do support.
        //
        return true;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>We only support TRANSACTION_SERIALIZABLE and TRANSACTION_READ_COMMITTED before 8.0; from 8.0
     * READ_UNCOMMITTED and REPEATABLE_READ are accepted aliases for READ_COMMITTED.</p>
     */
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return (level == Connection.TRANSACTION_SERIALIZABLE);
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return true;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    /**
     * <p>Does a data definition statement within a transaction force the transaction to commit? It seems
     * to mean something like:</p>
     *
     * <pre>
     * CREATE TABLE T (A INT);
     * INSERT INTO T (A) VALUES (2);
     * BEGIN;
     * UPDATE T SET A = A + 1;
     * CREATE TABLE X (A INT);
     * SELECT A FROM T INTO X;
     * COMMIT;
     * </pre>
     *
     * <p>Does the CREATE TABLE call cause a commit? The answer is no.</p>
     *
     * @return true if so
     * @throws SQLException if a database access error occurs
     */
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    /* lots of unsupported stuff... */
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        // indicates that
        return true;
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersDeletesAreVisible(int i) throws SQLException {
        return false;
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean deletesAreDetected(int i) throws SQLException {
        return false;
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    public boolean supportsRefCursors() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    /**
     * We support cursors for gets only it seems. I dont see a method to get a positioned delete.
     *
     * @return false
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsPositionedDelete() throws SQLException {
        return false; // For now...
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        return false; // For now...
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsSelectForUpdate() throws SQLException {
        return true;
    }

    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsUnion() throws SQLException {
        return true; // since 6.3
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc} In Redshift, Cursors are only open within transactions.
     */
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    /**
     * {@inheritDoc}
     * <p>Can statements remain open across commits? They may, but this driver cannot guarantee that. In
     * further reflection. we are talking a Statement object here, so the answer is yes, since the
     * Statement is only a vehicle to ExecSQL()</p>
     *
     * @return true
     */
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>Can statements remain open across rollbacks? They may, but this driver cannot guarantee that.
     * In further contemplation, we are talking a Statement object here, so the answer is yes, since
     * the Statement is only a vehicle to ExecSQL() in Connection</p>
     *
     * @return true
     */
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    public boolean supportsConvert() throws SQLException {
        return false;
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    /*
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    /*
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This grammar is defined at:
     * <a href="http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm">
     * http://www.microsoft.com/msdn/sdk/platforms/doc/odbc/src/intropr.htm</a></p>
     *
     * <p>In Appendix C. From this description, we seem to support the ODBC minimal (Level 0) grammar.</p>
     *
     * @return true
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Does this driver support the Core ODBC SQL grammar. We need SQL-92 conformance for this.
     *
     * @return false
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    /**
     * Does this driver support the Extended (Level 2) ODBC SQL grammar. We don't conform to the Core
     * (Level 1), so we can't conform to the Extended SQL Grammar.
     *
     * @return false
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    /**
     * Does this driver support the ANSI-92 entry level SQL grammar? All JDBC Compliant drivers must
     * return true. We currently report false until 'schema' support is added. Then this should be
     * changed to return true, since we will be mostly compliant (probably more compliant than many
     * other databases) And since this is a requirement for all JDBC drivers we need to get to the
     * point where we can return true.
     *
     * @return true
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return false
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * @return false
     */
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    /*
     * Is the SQL Integrity Enhancement Facility supported? Our best guess is that this means support
     * for constraints
     *
     * @return true
     *
     * @exception SQLException if a database access error occurs
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return true
     */
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }


    public boolean allProceduresAreCallable() throws SQLException {
        return true; // For now...
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return true; // For now...
    }


    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    /**
     * Does the database store tables in a local file? No - it stores them in a file on the server.
     *
     * @return true if so
     * @throws SQLException if a database access error occurs
     */
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    /**
     * Does the database use a file for each table? Well, not really, since it doesn't use local files.
     *
     * @return true if so
     * @throws SQLException if a database access error occurs
     */
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /**
     * Does the database treat mixed case unquoted SQL identifiers as case sensitive and as a result
     * store them in mixed case? A JDBC-Compliant driver will always return false.
     *
     * @return true if so
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return true;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Does the database treat mixed case quoted SQL identifiers as case sensitive and as a result
     * store them in mixed case? A JDBC compliant driver will always return true.
     *
     * @return true if so
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
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
        return 0;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }
}