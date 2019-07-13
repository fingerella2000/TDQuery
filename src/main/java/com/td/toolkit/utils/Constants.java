package com.td.toolkit.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Constants {

    final static Logger logger = LoggerFactory.getLogger(Constants.class);
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

    final public static Properties TABLE_COLUMNS = new Properties();

    public void loadTableColumns() {
        // using the property file to define the table schema
        InputStream is = this.getClass().getResourceAsStream("/table_schema.properties");
        if (is != null ) {
            try {
                TABLE_COLUMNS.load(is);
            } catch (IOException e) {
                logger.debug("table_schema.properties file is not set correctly");
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.error("table_schema.properties file is not closed correctly.", e);
                }
            }
        }
//        SCHEMA.put("time", "bigint");
//        SCHEMA.put("ordernumber", "bigint");
//        SCHEMA.put("customernumber", "bigint");
//        SCHEMA.put("orderdate", "varchar");
//        SCHEMA.put("shippeddate", "varchar");
//        SCHEMA.put("status", "varchar");
    }

}
