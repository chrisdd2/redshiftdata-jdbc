package dev.chrisdd.redshiftdata.config;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClientBuilder;

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

    String database;
    String workgroupName;
    String clusterIdentifier;
    String dbUser;
    String secretArn;
    int networkTimeout;

    String awsProfileName;

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
                new DriverPropertyInfo(NETWORK_TIMEOUT_PROPERTY,"60000")
        };
    }

    public void setProperties(String prop,String value) throws NumberFormatException{
        switch (prop){
            case DATABASE_PROPERTY: setDatabase(value);break;
            case WORKGROUP_NAME_PROPERTY: setWorkgroupName(value);break;
            case CLUSTER_IDENTIFIER_PROPERTY: setClusterIdentifier(value);break;
            case SECRET_ARN_PROPERTY: setSecretArn(value);break;
            case AWS_PROFILE_NAME_PROPERTY: setAwsProfileName(value);break;
            case NETWORK_TIMEOUT_PROPERTY: setNetworkTimeout(Integer.parseUnsignedInt(value));break;
        }

    }


    public RedshiftConfiguration(Properties props) throws SQLException{
        props.forEach((k,v) -> this.setProperties(k.toString(),v.toString()));

    }

    public RedshiftDataClient getClient(){
        RedshiftDataClientBuilder b = RedshiftDataClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder());
        if (this.awsProfileName != null && !this.awsProfileName.isEmpty())
            b = b.credentialsProvider(ProfileCredentialsProvider.create(this.awsProfileName));
        return b.build();
    }
}
