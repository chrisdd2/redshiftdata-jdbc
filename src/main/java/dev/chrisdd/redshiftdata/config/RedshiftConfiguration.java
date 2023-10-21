package dev.chrisdd.redshiftdata.config;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class RedshiftConfiguration {
    static final String DATABASE_PROPERTY = "database";
    static final String WORKGROUP_NAME_PROPERTY = "workgroupName";
    static final String CLUSTER_IDENTIFIER_PROPERTY = "clusterIdentifier";
    static final String DBUSER_PROPERTY = "dbUser";
    static final String SECRET_ARN_PROPERTY = "secretArn";
    static final String NETWORK_TIMEOUT_PROPERTY = "networkTimeout";

    static final String AWS_PROFILE_NAME_PROPERTY = "profile";

    static final String AWS_ACCESS_KEY_PROPERTY = "accessKey";
    static final String AWS_SECRET_KEY_PROPERTY = "secretKey";
    static final String AWS_SESSION_TOKEN_PROPERTY = "sessionToken";

    String database;
    String workgroupName;
    String clusterIdentifier;
    String dbUser;
    String secretArn;
    int networkTimeout;

    String awsProfileName;
    String awsAccessKey;
    String awsSecretKey;
    String awsSessionToken;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getWorkgroupName() {
        return workgroupName;
    }

    public void setWorkgroupName(String workgroupName) {
        this.workgroupName = workgroupName;
    }

    public String getClusterIdentifier() {
        return clusterIdentifier;
    }

    public void setClusterIdentifier(String clusterIdentifier) {
        this.clusterIdentifier = clusterIdentifier;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getSecretArn() {
        return secretArn;
    }

    public void setSecretArn(String secretArn) {
        this.secretArn = secretArn;
    }

    public int getNetworkTimeout() {
        return networkTimeout;
    }

    public void setNetworkTimeout(int networkTimeout) {
        this.networkTimeout = networkTimeout;
    }

    public String getAwsProfileName() {
        return awsProfileName;
    }

    public void setAwsProfileName(String awsProfileName) {
        this.awsProfileName = awsProfileName;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getAwsSessionToken() {
        return awsSessionToken;
    }

    public void setAwsSessionToken(String awsSessionToken) {
        this.awsSessionToken = awsSessionToken;
    }

    public static DriverPropertyInfo[] getPropertyInfo() {
        DriverPropertyInfo database = new DriverPropertyInfo(DATABASE_PROPERTY,"");
        database.required = true;
        return new DriverPropertyInfo[]{
                database,
                new DriverPropertyInfo(WORKGROUP_NAME_PROPERTY,""),
                new DriverPropertyInfo(CLUSTER_IDENTIFIER_PROPERTY,""),
                new DriverPropertyInfo(DBUSER_PROPERTY,""),
                new DriverPropertyInfo(SECRET_ARN_PROPERTY,""),
                new DriverPropertyInfo(AWS_PROFILE_NAME_PROPERTY,""),
                new DriverPropertyInfo(AWS_ACCESS_KEY_PROPERTY,""),
                new DriverPropertyInfo(AWS_SECRET_KEY_PROPERTY,""),
                new DriverPropertyInfo(AWS_SESSION_TOKEN_PROPERTY,""),
                new DriverPropertyInfo(NETWORK_TIMEOUT_PROPERTY,"60000")
        };
    }

    public void setProperties(String prop,String value) throws NumberFormatException{
        switch (prop){
            case DATABASE_PROPERTY: setDatabase(value);break;
            case WORKGROUP_NAME_PROPERTY: setWorkgroupName(value);break;
            case CLUSTER_IDENTIFIER_PROPERTY: setClusterIdentifier(value);break;
            case SECRET_ARN_PROPERTY: setSecretArn(value);break;
            case AWS_ACCESS_KEY_PROPERTY: setAwsAccessKey(value);break;
            case AWS_SECRET_KEY_PROPERTY: setAwsSecretKey(value);break;
            case AWS_SESSION_TOKEN_PROPERTY: setAwsSessionToken(value);break;
            case AWS_PROFILE_NAME_PROPERTY: setAwsProfileName(value);break;
            case NETWORK_TIMEOUT_PROPERTY: setNetworkTimeout(Integer.parseUnsignedInt(value));break;
        }

    }


    public RedshiftConfiguration(String database, Properties props) throws SQLException{
        setDatabase(database);
        props.forEach((k,v) -> this.setProperties(k.toString(),v.toString()));
        if (database.isEmpty()){
            throw new SQLException("database must not be empty");
        }
        if (workgroupName.isEmpty() && clusterIdentifier.isEmpty()){
            throw new SQLException("one of workgroupName, clusterIdentifier must be set");
        }

    }
}
