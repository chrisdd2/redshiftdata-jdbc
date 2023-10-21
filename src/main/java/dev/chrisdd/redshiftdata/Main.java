package dev.chrisdd.redshiftdata;

import java.util.regex.Matcher;

public class Main {
    public static void main(String[] args) {
       System.out.println(RedshiftDriver.JDBC_URL.pattern());
       Matcher k = RedshiftDriver.JDBC_URL.matcher("jdbc:redshiftdata:database");
       System.out.println(k.matches());
        System.out.println(k.group(1));

    }
}
