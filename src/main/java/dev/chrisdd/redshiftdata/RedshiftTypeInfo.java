package dev.chrisdd.redshiftdata;


import software.amazon.awssdk.services.redshiftdata.model.ColumnMetadata;
import software.amazon.awssdk.services.redshiftdata.model.Field;
import software.amazon.awssdk.services.redshiftdata.model.GetStatementResultResponse;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedshiftTypeInfo {


//        TYPE_NAME String => Type name
//        DATA_TYPE int => SQL data type from java.sql.Types
//        PRECISION int => maximum precision
//        LITERAL_PREFIX String => prefix used to quote a literal (may be null)
//        LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
//        CREATE_PARAMS String => parameters used in creating the type (may be null)
//        NULLABLE short => can you use NULL for this type.
//                typeNoNulls - does not allow NULL values
//        typeNullable - allows NULL values
//        typeNullableUnknown - nullability unknown
//        CASE_SENSITIVE boolean=> is it case sensitive.
//                SEARCHABLE short => can you use "WHERE" based on this type:
//        typePredNone - No support
//        typePredChar - Only supported with WHERE .. LIKE
//        typePredBasic - Supported except for WHERE .. LIKE
//        typeSearchable - Supported for all WHERE ..
//        UNSIGNED_ATTRIBUTE boolean => is it unsigned.
//                FIXED_PREC_SCALE boolean => can it be a money value.
//        AUTO_INCREMENT boolean => can it be used for an auto-increment value.
//                LOCAL_TYPE_NAME String => localized version of type name (may be null)
//        MINIMUM_SCALE short => minimum scale supported
//        MAXIMUM_SCALE short => maximum scale supported
//        SQL_DATA_TYPE int => unused
//        SQL_DATETIME_SUB int => unused
//        NUM_PREC_RADIX int => usually 2 or 10
    String typeName;
    int dataType;
    int precision;
    String literalPrefix;
    String literalSuffix;
    String createParams;
    int nullable;
    boolean caseSensitive;

    int searchable;
    boolean unsignedAttribute;
    boolean autoIncrement;
    String localTypeName;
    int minimumScale;
    int maximumScale;

    int numericPrecisionRadix;

    public String getTypeName() {
        return typeName;
    }

    public int getDataType() {
        return dataType;
    }

    public int getPrecision() {
        return precision;
    }

    public String getLiteralPrefix() {
        return literalPrefix;
    }

    public String getLiteralSuffix() {
        return literalSuffix;
    }

    public String getCreateParams() {
        return createParams;
    }

    public int getNullable() {
        return nullable;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public int getSearchable() {
        return searchable;
    }

    public boolean isUnsignedAttribute() {
        return unsignedAttribute;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public String getLocalTypeName() {
        return localTypeName;
    }

    public int getMinimumScale() {
        return minimumScale;
    }

    public int getMaximumScale() {
        return maximumScale;
    }

    public int getNumericPrecisionRadix() {
        return numericPrecisionRadix;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public void setLiteralPrefix(String literalPrefix) {
        this.literalPrefix = literalPrefix;
    }

    public void setLiteralSuffix(String literalSuffix) {
        this.literalSuffix = literalSuffix;
    }

    public void setCreateParams(String createParams) {
        this.createParams = createParams;
    }

    public void setNullable(int nullable) {
        this.nullable = nullable;
    }

    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public void setSearchable(int searchable) {
        this.searchable = searchable;
    }

    public void setUnsignedAttribute(boolean unsignedAttribute) {
        this.unsignedAttribute = unsignedAttribute;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public void setLocalTypeName(String localTypeName) {
        this.localTypeName = localTypeName;
    }

    public void setMinimumScale(int minimumScale) {
        this.minimumScale = minimumScale;
    }

    public void setMaximumScale(int maximumScale) {
        this.maximumScale = maximumScale;
    }

    public void setNumericPrecisionRadix(int numericPrecisionRadix) {
        this.numericPrecisionRadix = numericPrecisionRadix;
    }

    public RedshiftTypeInfo(String typeName, int dataType, int precision, String literalPrefix, String literalSuffix, String createParams, int nullable, boolean caseSensitive, int searchable, boolean unsignedAttribute, boolean autoIncrement, String localTypeName, int minimumScale, int maximumScale, int numericPrecisionRadix) {
        this.typeName = typeName;
        this.dataType = dataType;
        this.precision = precision;
        this.literalPrefix = literalPrefix;
        this.literalSuffix = literalSuffix;
        this.createParams = createParams;
        this.nullable = nullable;
        this.caseSensitive = caseSensitive;
        this.searchable = searchable;
        this.unsignedAttribute = unsignedAttribute;
        this.autoIncrement = autoIncrement;
        this.localTypeName = localTypeName;
        this.minimumScale = minimumScale;
        this.maximumScale = maximumScale;
        this.numericPrecisionRadix = numericPrecisionRadix;
    }


    public RedshiftTypeInfo alias(String alias){
        return new RedshiftTypeInfo(
                alias,
                this.dataType,
                this.precision,
                this.literalPrefix,
                this.literalSuffix,
                this.createParams,
                this.nullable,
                this.caseSensitive,
                this.searchable,
                this.unsignedAttribute,
                this.autoIncrement,
                alias,
                this.minimumScale,
                this.maximumScale,
                this.numericPrecisionRadix
        );
    }

    public static RedshiftTypeInfo SMALLINT = new RedshiftTypeInfo("SMALLINT", Types.SMALLINT,16,"","","", DatabaseMetaData.typeNullable,false,DatabaseMetaData.typeSearchable,false,true,"SMALLINT",16,16,2);
    public static RedshiftTypeInfo INT2 =SMALLINT.alias("INT2");
    public static RedshiftTypeInfo INTEGER = new RedshiftTypeInfo("INTEGER", Types.INTEGER,32,"","","", DatabaseMetaData.typeNullable,false,DatabaseMetaData.typeSearchable,false,true,"INTEGER",32,32,2);
    public static RedshiftTypeInfo INT = INTEGER.alias("INT");
    public static RedshiftTypeInfo INT4 = INTEGER.alias("INT4");
    public static RedshiftTypeInfo BIGINT = new RedshiftTypeInfo("BIGINT", Types.BIGINT,64,"","","", DatabaseMetaData.typeNullable,false,DatabaseMetaData.typeSearchable,false,true,"BIGINT",64,64,2);
    public static RedshiftTypeInfo INT8 = BIGINT.alias("INT8");

    public static RedshiftTypeInfo DECIMAL = new RedshiftTypeInfo("DECIMAL", Types.DECIMAL,38,"","","", DatabaseMetaData.typeNullable,false,DatabaseMetaData.typeSearchable,false,true,"DECIMAL",20,20,2);
    public static RedshiftTypeInfo NUMERIC = DECIMAL.alias("NUMERIC");
    public static RedshiftTypeInfo REAL = new RedshiftTypeInfo("REAL", Types.REAL,24,"","","", DatabaseMetaData.typeNullable,false,DatabaseMetaData.typeSearchable,false,true,"REAL",0,0,2);
    public static RedshiftTypeInfo FLOAT4 = REAL.alias("FLOAT4") ;
    public static RedshiftTypeInfo DOUBLE_PRECISION = new RedshiftTypeInfo("DOUBLE PRECISION", Types.DOUBLE,53,"","","", DatabaseMetaData.typeNullable,false,DatabaseMetaData.typeSearchable,false,true,"REAL",0,0,2);
    public static RedshiftTypeInfo FLOAT8 = DOUBLE_PRECISION.alias("FLOAT8") ;
    public static RedshiftTypeInfo FLOAT = DOUBLE_PRECISION.alias("FLOAT") ;

    public static RedshiftTypeInfo BOOLEAN =  new RedshiftTypeInfo("BOOLEAN", Types.BOOLEAN,1,"","","", DatabaseMetaData.typeNullable,false,DatabaseMetaData.typeSearchable,false,true,"BOOLEAN",0,0,0);
    public static RedshiftTypeInfo BOOL =  BOOLEAN.alias("BOOL");

    public static ResultSet getTypeInfo(RedshiftStatement stmt) throws SQLException {
        ColumnMetadata[] columns = new ColumnMetadata[]{
                ColumnMetadata.builder()
                        .name("TYPE_NAME")
                        .typeName("varchar")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("DATA_TYPE")
                        .typeName("integer")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("PRECISION")
                        .typeName("integer")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("LITERAL_PREFIX")
                        .typeName("varchar")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("LITERAL_SUFFIX")
                        .typeName("varchar")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("CREATE_PARAMS")
                        .typeName("varchar")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("NULLABLE")
                        .typeName("integer")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("CASE_SENSITIVE")
                        .typeName("bool")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SEARCHABLE")
                        .typeName("integer")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("UNSIGNED_ATTRIBUTE")
                        .typeName("bool")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("AUTO_INCREMENT")
                        .typeName("bool")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("LOCAL_TYPE_NAME")
                        .typeName("varchar")
                        .nullable(ResultSetMetaData.columnNullable)
                        .build(),
                ColumnMetadata.builder()
                        .name("MINIMUM_SCALE")
                        .typeName("bool")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("MAXIMUM_SCALE")
                        .typeName("integer")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SQL_DATA_TYPE")
                        .typeName("integer")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("SQL_DATETIME_SUB")
                        .typeName("integer")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build(),
                ColumnMetadata.builder()
                        .name("NUM_PREC_RADIX")
                        .typeName("integer")
                        .nullable(ResultSetMetaData.columnNoNulls)
                        .build()
        };
        List<List<Field>> records = Stream.of(
    SMALLINT , INT2 , INTEGER , INT , INT4 , BIGINT ,
    INT8 , DECIMAL , NUMERIC , REAL , FLOAT4 , DOUBLE_PRECISION ,
    FLOAT8 , FLOAT , BOOLEAN , BOOL
                )
                .map(d -> Arrays.asList(
                        Field.builder().stringValue(d.typeName).build(),
                        Field.builder().longValue((long) d.dataType).build(),
                        Field.builder().longValue((long) d.precision).build(),
                        Field.builder().stringValue(d.literalPrefix).build(),
                        Field.builder().stringValue(d.literalSuffix).build(),
                        Field.builder().stringValue(d.createParams).build(),
                        Field.builder().longValue((long)d.nullable).build(),
                        Field.builder().longValue((long)d.searchable).build(),
                        Field.builder().booleanValue(d.caseSensitive).build(),
                        Field.builder().booleanValue(d.unsignedAttribute).build(),
                        Field.builder().booleanValue(d.autoIncrement).build(),
                        Field.builder().stringValue(d.localTypeName).build(),
                        Field.builder().longValue((long)d.minimumScale).build(),
                        Field.builder().longValue((long)d.maximumScale).build(),
                        Field.builder().isNull(true).build(),
                        Field.builder().isNull(true).build(),
                        Field.builder().longValue((long)d.numericPrecisionRadix).build()
                ))
                .collect(Collectors.toList());
        records.forEach(System.out::println);
        return new RedshiftResultSet(stmt, Collections.singletonList(
                GetStatementResultResponse.builder()
                        .columnMetadata(columns)
                        .records(records)
                        .totalNumRows((long) records.size())
                        .build()).iterator());

    }
}
//    SMALLINT	INT2
//    INTEGER	INT, INT4
//    BIGINT	INT8
//    DECIMAL	NUMERIC
//    REAL	FLOAT4
//    DOUBLE PRECISION	FLOAT8, FLOAT
//    BOOLEAN	BOOL
//    CHAR	CHARACTER, NCHAR, BPCHAR
//    VARCHAR	CHARACTER VARYING, NVARCHAR, TEXT
//    DATE
//    TIMESTAMP	Timestamp without time zone
//    TIMESTAMPTZ	Timestamp with time zone
//    SUPER
//    TIME	Time without time zone
//    TIMETZ	Time with time zone
//    VARBYTE	VARBINARY, BINARY VARYING
