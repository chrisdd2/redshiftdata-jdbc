package dev.chrisdd.redshiftdata.ripped;

// rip from https://raw.githubusercontent.com/aws/amazon-redshift-jdbc-driver/master/src/main/java/com/amazon/redshift/jdbc/EscapedFunctions.java
/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */


/**
 * This class stores supported escaped function.
 *
 * @author Xavier Poinsard
 */
@Deprecated
public class EscapedFunctions {
    // numeric functions names
    public static final String ABS = "abs";
    public static final String ACOS = "acos";
    public static final String ASIN = "asin";
    public static final String ATAN = "atan";
    public static final String ATAN2 = "atan2";
    public static final String CEILING = "ceiling";
    public static final String COS = "cos";
    public static final String COT = "cot";
    public static final String DEGREES = "degrees";
    public static final String EXP = "exp";
    public static final String FLOOR = "floor";
    public static final String LOG = "log";
    public static final String LOG10 = "log10";
    public static final String MOD = "mod";
    public static final String PI = "pi";
    public static final String POWER = "power";
    public static final String RADIANS = "radians";
    public static final String RANDOM = "random";
    public static final String ROUND = "round";
    public static final String SIGN = "sign";
    public static final String SIN = "sin";
    public static final String SQRT = "sqrt";
    public static final String TAN = "tan";
    public static final String TRUNCATE = "truncate";

    // string function names
    public static final String ASCII = "ascii";
    public static final String CHAR = "char";
    public static final String CHAR_LENGTH = "char_length";
    public static final String CHARACTER_LENGTH = "character_length";
    public static final String CONCAT = "concat";
    public static final String INSERT = "insert"; // change arguments order
    public static final String LCASE = "lcase";
    public static final String LEFT = "left";
    public static final String LENGTH = "length";
    public static final String LOCATE = "locate"; // the 3 args version duplicate args
    public static final String LTRIM = "ltrim";
    public static final String OCTET_LENGTH = "octet_length";
    public static final String POSITION  = "position";
    public static final String REPEAT = "repeat";
    public static final String REPLACE = "replace";
    public static final String RIGHT = "right"; // duplicate args
    public static final String RTRIM = "rtrim";
    public static final String SPACE = "space";
    public static final String SUBSTRING = "substring";
    public static final String UCASE = "ucase";
    // soundex is implemented on the server side by
    // the contrib/fuzzystrmatch module. We provide a translation
    // for this in the driver, but since we don't want to bother with run
    // time detection of this module's installation we don't report this
    // method as supported in DatabaseMetaData.
    // difference is currently unsupported entirely.

    // date time function names
    public static final String CURDATE = "curdate";
    public static final String CURTIME = "curtime";
    public static final String DAYNAME = "dayname";
    public static final String DAYOFMONTH = "dayofmonth";
    public static final String DAYOFWEEK = "dayofweek";
    public static final String DAYOFYEAR = "dayofyear";
    public static final String HOUR = "hour";
    public static final String MINUTE = "minute";
    public static final String MONTH = "month";
    public static final String MONTHNAME = "monthname";
    public static final String NOW = "now";
    public static final String QUARTER = "quarter";
    public static final String SECOND = "second";
    public static final String WEEK = "week";
    public static final String YEAR = "year";
    // for timestampadd and timestampdiff the fractional part of second is not supported
    // by the backend
    // timestampdiff is very partially supported
    public static final String TIMESTAMPADD = "timestampadd";
    public static final String TIMESTAMPDIFF = "timestampdiff";

    // constants for timestampadd and timestampdiff
    public static final String SQL_TSI_ROOT = "SQL_TSI_";
    public static final String SQL_TSI_DAY = "DAY";
    public static final String SQL_TSI_FRAC_SECOND = "FRAC_SECOND";
    public static final String SQL_TSI_HOUR = "HOUR";
    public static final String SQL_TSI_MINUTE = "MINUTE";
    public static final String SQL_TSI_MONTH = "MONTH";
    public static final String SQL_TSI_QUARTER = "QUARTER";
    public static final String SQL_TSI_SECOND = "SECOND";
    public static final String SQL_TSI_WEEK = "WEEK";
    public static final String SQL_TSI_YEAR = "YEAR";

    // system functions
    public static final String DATABASE = "database";
    public static final String IFNULL = "ifnull";
    public static final String USER = "user";
}