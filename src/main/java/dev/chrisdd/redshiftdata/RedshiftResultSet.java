package dev.chrisdd.redshiftdata;

import software.amazon.awssdk.services.redshiftdata.model.Field;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultResponse;
import software.amazon.awssdk.services.redshiftdata.paginators.GetStatementResultIterable;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.chrono.IsoChronology;
import java.time.format.*;
import java.time.temporal.ChronoField;
import java.util.*;

class RedshiftResultSet implements ResultSet {

    //        "stringValue": "2023-10-23"
    //        "stringValue": "09:26:38.779304+00"
    //        "stringValue": "2023-10-23 09:26:38.779304+00"
    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    public static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            //        "stringValue": "2023-10-23 09:26:38.779304+00"
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendLiteral(" ")
            .appendValue(ChronoField.HOUR_OF_DAY,2)
            .appendLiteral(":")
            .appendValue(ChronoField.MINUTE_OF_DAY,2)
            .appendLiteral(":")
            .appendValue(ChronoField.SECOND_OF_DAY,2)
            .appendLiteral(".")
            .appendValue(ChronoField.MICRO_OF_SECOND)
            .appendZoneOrOffsetId()
            .toFormatter();
    public static final DateTimeFormatter DATE_FORMATTER =new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .toFormatter();


    private final RedshiftStatement stmt;
    private List<List<Field>> resultRows;

    private List<Field> currentRow;
    private int totalResultRows;
    private int rowIndex;
    private int lastColumn;

    private RedshiftResultSetMetadata metadata;

    private Object fieldToObject(Field fld) throws SQLException{
        String fieldName = fld.type().name();
        switch (fieldName) {
            case "BLOB_VALUE":
                return fld.blobValue();
            case "BOOLEAN_VALUE":
                return fld.booleanValue();
            case "DOUBLE_VALUE":
                return fld.doubleValue();
            case "IS_NULL":
                return null;
            case "LONG_VALUE":
                return fld.longValue();
            case "STRING_VALUE":
                return fld.stringValue();
            default:
                throw new SQLException("unhandled type " + fieldName);
        }
    }

    private void processResponse(Iterator<GetStatementResultResponse> iter) throws SQLException {
        List<List<Field>> rows = new ArrayList<>();

        while (iter.hasNext()){
            GetStatementResultResponse resp = iter.next();
            // parse metadata in first response only
            if (this.metadata == null) {
                this.metadata = new RedshiftResultSetMetadata(resp);
                this.totalResultRows = Math.toIntExact(resp.totalNumRows());
            }
            rows.addAll(resp.records());
        }
        this.resultRows = rows;
        this.rowIndex=0;
    }

    public RedshiftResultSet(RedshiftStatement stmt, Iterator<GetStatementResultResponse> results) throws SQLException {
        this.stmt = stmt;
        this.processResponse(results);
    }

    @Override
    public boolean next() throws SQLException {
        if (this.rowIndex == this.totalResultRows){
            this.afterLast();
            return false;
        }
        this.currentRow = this.resultRows.get(this.rowIndex++);
        this.lastColumn = -1;
        return true;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean wasNull() throws SQLException {
        if (this.currentRow == null){
            throw new SQLException("you have to call next at least once");
        }
        if (this.lastColumn == -1){
            throw new SQLException("you have to read at least one column");
        }
        return this.currentRow.get(this.lastColumn).isNull();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return String.valueOf(this.getObject(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return this.getObject(columnIndex,Boolean.class);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return this.getObject(columnIndex,Long.class).byteValue();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return this.getObject(columnIndex,Long.class).shortValue();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return this.getObject(columnIndex,Long.class).intValue();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return this.getObject(columnIndex,Long.class);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return this.getObject(columnIndex,Float.class);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return this.getObject(columnIndex,Double.class);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return new BigDecimal(getString(columnIndex));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return Date.valueOf(LocalDate.parse(getString(columnIndex),DATE_FORMATTER));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return Time.valueOf(LocalTime.parse(getString(columnIndex),TIME_FORMATTER));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return Timestamp.valueOf(LocalDateTime.parse(getString(columnIndex),TIMESTAMP_FORMATTER));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getObject(columnLabel,String.class);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }
    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");

    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.metadata;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return fieldToObject(getField(columnIndex));
    }

    private Field getField(int columnIndex) throws SQLException {
        this.lastColumn = columnIndex-1;
        return this.currentRow.get(columnIndex-1);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return this.getObject(this.findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int n = this.metadata.getColumnCount();
        for (int i=1;i<=n;i++){
            if (columnLabel.equalsIgnoreCase(this.metadata.getColumnName(i)))
                return i;
        }
        throw new SQLException(String.format("column not %s found",columnLabel));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("no supported");
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.currentRow == null;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.rowIndex == this.totalResultRows +1;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return this.rowIndex == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return this.rowIndex == this.totalResultRows;
    }

    @Override
    public void beforeFirst() throws SQLException {
        this.rowIndex = 0;
        this.currentRow = null;
    }

    @Override
    public void afterLast() throws SQLException {
        this.rowIndex = this.totalResultRows + 1;
        this.currentRow = null;
    }

    @Override
    public boolean first() throws SQLException {
        return absolute(1);
    }

    @Override
    public boolean last() throws SQLException {
        return absolute(-1);
    }

    @Override
    public int getRow() throws SQLException {
        return this.rowIndex;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if( this.totalResultRows == 0 )
            return false;
        if ( row == -1)
            row = this.totalResultRows;
        if (row <= 0){
            throw new SQLException("huh row");
        }
        this.currentRow = this.resultRows.get(row-1);
        this.lastColumn = -1;
        this.rowIndex = row;
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (this.rowIndex + rows > this.totalResultRows)
            return false;
        return absolute(this.rowIndex + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        return this.absolute(this.rowIndex-1);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.stmt;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLException("not supported url");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLException("not supported url");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLException("not supported row id");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLException("not supported row id");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLException("not supported clob");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLException("not supported clob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLException("not supported sqlxml");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLException("not supported sqlxml");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLException("not supported nstring");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLException("not supported nstring");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLException("not supported ncharacter stream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLException("not supported ncharacter stream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }


    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Field field  = getField(columnIndex);
        if (field.isNull()){
            return null;
        }
        if (type.equals(Long.class)) {
            return type.cast(field.longValue());
        } else if (type.equals(Integer.class)) {
            return type.cast(field.longValue().intValue());
        } else if (type.equals(Short.class)) {
            return type.cast(field.longValue().shortValue());
        } else if (type.equals(Byte.class)) {
            return type.cast(field.longValue().byteValue());
        } else if (type.equals(String.class)) {
            return type.cast(field.stringValue());
        } else if (type.equals(Double.class)) {
            return type.cast(field.doubleValue());
        } else if (type.equals(Float.class)) {
            return type.cast(field.doubleValue().floatValue());
        } else if ( type.equals(BigDecimal.class)){
            return type.cast(new BigDecimal(field.stringValue()));
        } else if ( type.equals(Boolean.class)){
            return type.cast(field.booleanValue());
        }
        return type.cast(this.getObject(columnIndex));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return type.cast(this.getObject(columnLabel));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)){
            return iface.cast(this);
        }
        throw new SQLException(String.format("%s is not a wrapper for %s",getClass().getName(),iface.getName()));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }
}