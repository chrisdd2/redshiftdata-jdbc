package dev.chrisdd.redshiftdata;

import java.sql.*;
import java.util.Properties;
import java.util.regex.Matcher;

public class Main {

    public static void printResultSet(ResultSet results) throws SQLException {
        ResultSetMetaData meta = results.getMetaData();
        for (int i=1;i<=meta.getColumnCount();i++){
            System.out.print(meta.getColumnName(i));
            System.out.print("\t");
        }
        System.out.println();
        for (int i=1;i<=meta.getColumnCount();i++){
            System.out.print(meta.getColumnTypeName(i) +"(" + meta.getPrecision(i) + ")");
            System.out.print("\t");
        }
        System.out.println();

        while (results.next()){
            System.out.print("row: ");
            for (int i=1;i<=meta.getColumnCount();i++){
                System.out.print(results.getString(i));
                System.out.print("\t");
            }
            System.out.println();
        }
        System.out.println("==========");
    }
    public static void main(String[] args) throws SQLException {
//       System.out.println(RedshiftDriver.JDBC_URL.pattern());
//       Matcher k = RedshiftDriver.JDBC_URL.matcher("jdbc:redshiftdata:database");
//       System.out.println(k.matches());
//        System.out.println(k.group(1));
//        Connection con = DriverManager.getConnection("jdbc:redshiftdata:@development-redshiftserverless-workgroup/development-redshift-database?serverless=true");
        Connection con = new RedshiftDriver().connect("jdbc:redshiftdata:@development-redshiftserverless-workgroup/development-redshift-database?serverless=true&profile=development",new Properties());
//        ResultSet results = con.getMetaData().getTables("","","",null);
//        Statement stmt = con.createStatement();
//        ResultSet results = stmt.executeQuery("select * from pg_tables limit 10");
//        printResultSet(con.getMetaData().getSchemas());
//        printResultSet(con.getMetaData().getTypeInfo());
//        printResultSet(con.getMetaData().getTableTypes());
//        printResultSet(con.getMetaData().getTables("","","",null));
        printResultSet(con.getMetaData().getColumns("","raw","bdw_part_number",""));



    }
}
