package dev.chrisdd.redshiftdata;

import dev.chrisdd.redshiftdata.config.RedshiftConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedshiftDriver implements Driver {
    public static int MAJOR_VERSION = 0;
    public static int MINOR_VERSION = 1;

    public static Pattern JDBC_URL = Pattern.compile("^jdbc:redshiftdata(?::([a-zA-Z]\\w*))?$");


    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Matcher m = JDBC_URL.matcher(url);
        if (!m.matches())
            throw new SQLException("url not compatible");
        return new RedshiftConnection(new RedshiftConfiguration(m.group(1),info));
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