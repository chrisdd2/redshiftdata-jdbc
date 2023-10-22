package dev.chrisdd.redshiftdata;

import dev.chrisdd.redshiftdata.config.RedshiftConfiguration;

import java.sql.*;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedshiftDriver implements Driver {
    public static int MAJOR_VERSION = 0;
    public static int MINOR_VERSION = 1;

    public static Pattern JDBC_URL = Pattern.compile("^jdbc:redshiftdata:@(?<id>[\\w-]+)/(?<db>[\\w-]+)(?<params>\\?[\\w-=_]+)$");


    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Matcher m = JDBC_URL.matcher(url);
        if (!m.matches())
            throw new SQLException("url not compatible");
        String identifier=m.group("id");
        String database = m.group("db");
        String params = m.group("params");
        RedshiftConfiguration rd = new RedshiftConfiguration(info);
        rd.setDatabase(database);
        if (!params.isEmpty()){
            String[] paramList = params.substring(1).split("&");
            for (String param : paramList){
                String[] paramParts = param.split("=");
                String key = paramParts[0];
                String value = paramParts[1];
                if (key.equals("serverless") && value.equalsIgnoreCase("true")) {
                    rd.setWorkgroupName(identifier);
                    rd.setClusterIdentifier("");
                }else if (key.equals("secretArn") && !value.isEmpty()){
                    rd.setSecretArn(value);
                }else if (key.equals("dbUser") && !value.isEmpty()){
                    rd.setDbUser(value);
                }else if (key.equals("profile") && !value.isEmpty()){
                    rd.setAwsProfileName(value);
                }
            }
        }
        return new RedshiftConnection(rd);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return JDBC_URL.matcher(url).matches();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return RedshiftConfiguration.getPropertyInfo();
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("not using default logger");
    }

    private static Driver getDriver(){
        for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements(); ) {
            Driver d = e.nextElement();
            if (d.getClass() == RedshiftDriver.class) {
                return d;
            }
        }
        return null;
    }

    static {
        if (getDriver() == null) {
            try {
                DriverManager.registerDriver(new RedshiftDriver());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}