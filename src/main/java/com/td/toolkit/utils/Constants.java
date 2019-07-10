package com.td.toolkit.utils;

public class Constants {
    /**
     * Help message
     */
    final public static String MSG_HELP = "TDQuery-(version).jar [Options]... DatabaseName TableName \n Options:\n";
    final public static String MSG_MISSING_REQUIRED_ARGS = "DatabaseName and TableName are the only required arguments, please specify.\n ";

    final public static String INVALID_ENGINE = "Invalid engine type, only presto and hive are available.";
    final public static String INVALID_FORMAT = "Invalid engine type, only tsv and csv are available.";

    /**
     * Options' values
     */
    final public static String OUTPUT_FORMAT_TSV = "tsv"; // default
    final public static String OUTPUT_FORMAT_CSV = "csv";
    final public static String QUERY_ENGINE_PRESTO = "presto"; // default
    final public static String QUERY_ENGINE_HIVE = "hive";

}
