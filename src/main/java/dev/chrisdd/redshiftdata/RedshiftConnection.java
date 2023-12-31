package dev.chrisdd.redshiftdata;

import dev.chrisdd.redshiftdata.config.RedshiftConfiguration;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.model.*;
import software.amazon.awssdk.services.redshiftdata.paginators.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

class RedshiftConnection implements Connection {

    private final RedshiftConfiguration config;
    private final RedshiftDataClient client;
    private String schema;
    private String catalog;

    public RedshiftConnection(RedshiftConfiguration config){
        this.client = config.getClient();
        this.config = config;
        this.schema = null;
        this.catalog= config.getDatabase();
    }



    public ListDatabasesIterable listDatabase(){
        ListDatabasesRequest req = ListDatabasesRequest.builder()
                .database(this.config.getDatabase())
                .clusterIdentifier(this.config.getClusterIdentifier())
                .dbUser(this.config.getDbUser())
                .secretArn(this.config.getSecretArn())
                .workgroupName(this.config.getWorkgroupName())
                .build();
        return this.client.listDatabasesPaginator(req);
    }
    public ListTablesIterable listTables(String schemaPattern,String tablePattern){
        ListTablesRequest req = ListTablesRequest.builder()
                .database(this.config.getDatabase())
                .clusterIdentifier(this.config.getClusterIdentifier())
                .dbUser(this.config.getDbUser())
                .secretArn(this.config.getSecretArn())
                .workgroupName(this.config.getWorkgroupName())
                .connectedDatabase(this.config.getDatabase())
                .tablePattern(tablePattern)
                .schemaPattern(schemaPattern)
                .build();
        return this.client.listTablesPaginator(req);
    }

    public Iterator<DescribeTableResponse> describeTable(String schema,String name){
        DescribeTableRequest req = DescribeTableRequest.builder()
                .workgroupName(this.config.getWorkgroupName())
                .database(this.config.getDatabase())
                .connectedDatabase(this.config.getDatabase())
                .clusterIdentifier(this.config.getClusterIdentifier())
                .secretArn(this.config.getSecretArn())
                .dbUser(this.config.getDbUser())
                .schema(schema)
                .table(name)
                .build();
        return this.client.describeTablePaginator(req).iterator();
    }


    public ListSchemasIterable listSchemas(String schemaPattern){
        ListSchemasRequest req = ListSchemasRequest.builder()
                .database(this.config.getDatabase())
                .clusterIdentifier(this.config.getClusterIdentifier())
                .dbUser(this.config.getDbUser())
                .secretArn(this.config.getSecretArn())
                .workgroupName(this.config.getWorkgroupName())
                .connectedDatabase(this.config.getDatabase())
                .schemaPattern(schemaPattern)
                .build();
        return this.client.listSchemasPaginator(req);
    }

    private void cancelExecution(String id){
        CancelStatementRequest req = CancelStatementRequest.builder().id(id).build();
        try{
            this.client.cancelStatement(req);
        } catch (RedshiftDataException ignored){

        }
    }

    private void waitExecution(String id) throws InterruptedException, SQLException {
        int sleepDuration = 5;
        int totalSleep = 0;
        while (true){
            DescribeStatementResponse resp = describeExecution(id);
            switch (resp.status()){
                case PICKED:
                case SUBMITTED:
                case STARTED:
                    if (totalSleep > this.config.getNetworkTimeout()) {
                        cancelExecution(id);
                        throw new SQLException(String.format("query timed out after %d miliseconds",totalSleep));
                    }
                    sleepDuration = sleepDuration*2;
                    if (sleepDuration > 2000) sleepDuration = 2000;
                    Thread.sleep(sleepDuration);
                    totalSleep += sleepDuration;
                    continue;
                case FINISHED:
                    return;
                case FAILED:
                case ABORTED:
                    throw new SQLException(String.format("query failed/aborted %s %s",resp.redshiftQueryId(),resp.error()));
                case UNKNOWN_TO_SDK_VERSION:
                    throw new SQLException(String.format("unknown status %s",resp.statusAsString()));
            }
        }
    }

    private DescribeStatementResponse describeExecution(String id){
        DescribeStatementRequest req = DescribeStatementRequest.builder()
                .id(id)
                .build();
        return this.client.describeStatement(req);
    }

    private Iterator<GetStatementResultResponse> getStatementResult(String id){
        GetStatementResultRequest req = GetStatementResultRequest.builder()
                .id(id)
                .build();
        return this.client.getStatementResultPaginator(req).iterator();
    }

    private String executeSqlImpl(String query,SqlParameter[] parameters){
        ExecuteStatementRequest.Builder req = ExecuteStatementRequest.builder()
                .sql(query)
                .database(this.config.getDatabase())
                .clusterIdentifier(this.config.getClusterIdentifier())
                .dbUser(this.config.getDbUser())
                .secretArn(this.config.getSecretArn())
                .workgroupName(this.config.getWorkgroupName());
        if (parameters.length >0 )
            req = req.parameters(parameters);
        ExecuteStatementResponse resp = this.client.executeStatement(req.build());
        return resp.id();
    }

    public long executeSql(String query, SqlParameter[] params) throws SQLException, InterruptedException {
        String id = this.executeSqlImpl(query,params);
        waitExecution(id);
        return describeExecution(id).resultRows();
    }
    public Optional<Iterator<GetStatementResultResponse>> executeQuery(String query,SqlParameter[] params ) throws SQLException, InterruptedException {
        String id = this.executeSqlImpl(query,params);
        waitExecution(id);
        DescribeStatementResponse resp = describeExecution(id);
        if (resp.hasResultSet())
            return Optional.of(this.getStatementResult(id));
        return Optional.empty();
    }



    @Override
    public Statement createStatement() throws SQLException {
        return new RedshiftStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new RedshiftPreparedStatement(this,sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported call");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported commit");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported rollback");
    }

    @Override
    public void close() throws SQLException {
        this.client.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new RedshiftDatabaseMetadata(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported warnings");
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported call");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported type map");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported savepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported savepoint");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported savepoint");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported savepoint");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return this.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported call");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported clob");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported blob");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported blob");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported sqlxml");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return "redshift-data-jdbc";
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported array");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported struct");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return this.schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("not supported abort");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        this.config.setNetworkTimeout(milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return this.config.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface))
            return iface.cast(this);
        throw new SQLException(String.format("%s is not a wrapper for %s",getClass().getName(),iface.getName()));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    public RedshiftConfiguration getConfig(){
        return this.config;
    }
}