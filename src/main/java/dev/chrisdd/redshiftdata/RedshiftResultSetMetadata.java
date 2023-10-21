package dev.chrisdd.redshiftdata;

import software.amazon.awssdk.services.redshiftdata.model.ColumnMetadata;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultResponse;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

class RedshiftResultSetMetadata implements ResultSetMetaData {

    private final List<ColumnMetadata> metadata;

    public RedshiftResultSetMetadata(GetStatementResultResponse resp){
        this.metadata = resp.columnMetadata();
    }
    @Override
    public int getColumnCount() throws SQLException {
        return metadata.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return this.metadata.get(column).isCaseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return this.metadata.get(column).isCurrency();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return this.metadata.get(column).nullable() > 0? ResultSetMetaData.columnNullable: ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return this.metadata.get(column).isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return this.metadata.get(column).length();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return this.metadata.get(column).label();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return this.metadata.get(column).name();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return this.metadata.get(column).schemaName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return this.metadata.get(column).precision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return this.metadata.get(column).scale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return this.metadata.get(column).tableName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "redshift";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        String type = this.getColumnTypeName(column);
        switch(type){
            case "bigint":
            case "oid":
                return Types.BIGINT;
            case "boolean": return Types.BOOLEAN;
            case "char": return Types.CHAR;
            case "date": return Types.DATE;
            case "decimal": return Types.NUMERIC;
            case "double": return Types.DOUBLE;
            case "geometry": return Types.LONGVARBINARY;
            case "integer": return Types.INTEGER;
            case "super": return Types.LONGNVARCHAR;
            case "real": return Types.REAL;
            case "time": return Types.TIME;
            case "timetz": return Types.TIME_WITH_TIMEZONE;
            case "timestamp": return Types.TIMESTAMP;
            case "timestamptz": return Types.TIMESTAMP_WITH_TIMEZONE;
            default:  return Types.VARCHAR;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return this.metadata.get(column).typeName().toLowerCase();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        String type = this.getColumnTypeName(column);
        switch(type){
            case "bigint":
            case "oid":
                return Long.class.getName();
            case "boolean": return Boolean.class.getName();
            case "char": return Character.class.getName();
            case "real":
            case "decimal": return BigDecimal.class.getName();
            case "double": return Double.class.getName();
            case "integer": return Integer.class.getName();
            default:  return String.class.getName();
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface))
            return iface.cast(this);
        throw new SQLException(String.format("%s is not a wrapper for %s",getClass().getName(),iface.getName()));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }
}