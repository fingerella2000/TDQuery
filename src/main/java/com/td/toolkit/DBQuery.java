/**
 * Copyright (c) 2019 Bryan Luo. All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License, as
 * published by the Free Software Foundation.
 *
 */
package com.td.toolkit;

import com.td.toolkit.utils.ExactMatchParser;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A Command Line Interface to send query to Treasure Data.
 *
 * @author  Bryan Luo
 * @version 1.0
 * @since   1.0
 */
public class DBQuery {

    final static Logger logger = LoggerFactory.getLogger(DBQuery.class);

    public static void main(String[] args) {
        // create Options object
        Options options = new Options();

        // by default, adding -h and --help to guide user how to use the command line tool
        options.addOption( Option.builder("h").required(false).longOpt("help").desc("Tisplay help message").build() );
        options.addOption( Option.builder("c").required(false).longOpt("column").desc("Optional: The comma separated list of columns to restrict the result to. Return all columns if not specified.").hasArg(true).argName("col1,col2,...").valueSeparator(',').build() );
        options.addOption( Option.builder("f").required(false).longOpt("format").desc("Optional: The output format, csv(comma separated value) or tsv(tab separated value). tsv by default.").hasArg(true).argName("tsv/csv").build() );
        options.addOption( Option.builder("l").required(false).longOpt("limit").desc("Optional: The limit of records returned. Read all records if not specified.").hasArg(true).argName("number").build() );
        options.addOption( Option.builder("m").required(false).longOpt("min").desc("Optional: The minimum timestamp. NULL by default.").hasArg(true).argName("timestamp").build() );
        options.addOption( Option.builder("M").required(false).longOpt("MAX").desc("Optional: The maximum timestamp. NULL by default.").hasArg(true).argName("timestamp").build() );
        options.addOption( Option.builder("e").required(false).longOpt("engine").desc("Optional: The query engine, presto or hive. presto by default.").hasArg(true).argName("engine").build() );

        // output help message
        HelpFormatter formatter = new HelpFormatter();
        if (args.length == 0) {
            formatter.printHelp( "DBQuery [Options]... DatabaseName, TableName \n Options:\n", options );
            System.exit(0);
        }

//        CommandLineParser parser = new DefaultParser();
        CommandLineParser parser = new ExactMatchParser();

        try {
            CommandLine cmd = parser.parse( options, args );

            // parsing the requirement arguments for database name and table name
            List<String> parsed_args = cmd.getArgList();
            logger.debug("normal arguments: {}", parsed_args.toString());

            // output help message only when there is one option "-h" or "--help"
            if (cmd.getOptions().length == 1 && (cmd.hasOption('h') || cmd.hasOption("help"))) {
                // print help message
                formatter.printHelp( "DBQuery [Options]... DatabaseName, TableName \n Options:\n", options );
                System.exit(0);
            }

            // check column option
            String columns_value = "*";
            if (cmd.hasOption('c') || cmd.hasOption("column")) {
                // get column option value
                columns_value = cmd.getOptionValue("c");
                logger.debug("columns: {}", columns_value);
            }else {
                // select all columns if column option not specified
                logger.debug("selecting all columns");
            }

            // check format option
            String format = "tsv";
            if (cmd.hasOption('f') || cmd.hasOption("format")) {
                // get format option value
                format = cmd.getOptionValue("f");
                logger.debug("format: {}", format);
            } else {
                // the format is tsv by default
                logger.debug("format: {}", format);
            }

        }catch (ParseException e) {
            logger.error( "Invalid arguments: " + e.getMessage() );
            // print help message to guide user how to use the command line tool
            formatter.printHelp( "DBQuery [Options]... DatabaseName, TableName \n Options:\n", options );
            // logging the error for internal analysis
            logger.error("An exception occurred!", e);
            System.exit(-1);
        }

//        DBQuery query = new DBQuery();
//        query.runQuery();
        System.exit(0);
    }

    public int runQuery() {
        String name = "Treasure Data";
        String greeting = "Welcome to";
        logger.info("{} {}.", greeting, name);
        return 0;
    }
}
