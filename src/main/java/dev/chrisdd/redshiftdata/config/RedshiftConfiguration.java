package dev.chrisdd.redshiftdata.config;

public interface RedshiftConfiguration {
    public String getDatabase();
    public String getWorkgroupName();
    public String getClusterIdentifier();

    public String getDbUser();

    public String getSecretArn();


    public int getNetworkTimeout();
    public void setNetworkTimeout(int timeoutMilliseconds);
}
