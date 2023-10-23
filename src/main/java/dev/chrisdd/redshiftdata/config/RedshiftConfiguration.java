package dev.chrisdd.redshiftdata.config;

import dev.chrisdd.redshiftdata.RedshiftDriver;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient;
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClientBuilder;
import software.amazon.awssdk.services.redshiftdata.model.RedshiftDataRequest;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;

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
    String url;

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

    public String getUrl(){
        return this.url;
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


    public RedshiftConfiguration(String url,Properties props) throws SQLException{
        Matcher m = RedshiftDriver.JDBC_URL.matcher(url);
        if (!m.matches())
            throw new SQLException("url not compatible");
        String identifier=m.group("id");
        String database = m.group("db");
        String params = m.group("params");
        this.setDatabase(database);
        if (!params.isEmpty()){
            String[] paramList = params.substring(1).split("&");
            for (String param : paramList){
                String[] paramParts = param.split("=");
                String key = paramParts[0];
                String value = paramParts[1];
                if (key.equals("serverless") && value.equalsIgnoreCase("true")) {
                    this.setWorkgroupName(identifier);
                    this.setClusterIdentifier("");
                }else if (key.equals("secretArn") && !value.isEmpty()){
                    this.setSecretArn(value);
                }else if (key.equals("dbUser") && !value.isEmpty()){
                    this.setDbUser(value);
                }else if (key.equals("profile") && !value.isEmpty()){
                    this.setAwsProfileName(value);
                }
            }
        }
        props.forEach((k,v) -> this.setProperties(k.toString(),v.toString()));
        if (this.getNetworkTimeout() == 0) {
            this.setNetworkTimeout(60*1000);
        }
        this.url = url;
    }

    public RedshiftDataClient getClient(){
        RedshiftDataClientBuilder b = RedshiftDataClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder());
        if (this.awsProfileName != null && !this.awsProfileName.isEmpty())
            b = b.credentialsProvider(ProfileCredentialsProvider.create(this.awsProfileName));
        return b.build();
    }


}
