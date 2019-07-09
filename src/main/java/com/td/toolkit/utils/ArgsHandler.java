/**
 * Copyright (c) 2019 Bryan Luo. All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License, as
 * published by the Free Software Foundation.
 *
 */
package com.td.toolkit.utils;


import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * Provide methods to handler arguments specified in the command line.
 *
 * @author  Bryan Luo
 * @version 1.0
 * @since   1.0
 *
 */
public class ArgsHandler {

    final static Logger logger = LoggerFactory.getLogger(ArgsHandler.class);

    public ArgsHandler() {
    }

    /**
     * Create option list using arguments passed from command line
     * @param args arguments specified in the command line
     * @return option list
     *
     */
    public Options buildOptions(String[] args) {

        // create Options object
        Options options = new Options();

        // by default, adding -h and --help to guide user how to use the command line tool
        options.addOption( Option.builder("h").required(false).longOpt("help").desc("Display help message").build() );
        options.addOption( Option.builder("c").required(false).longOpt("column").desc("Optional: The comma separated list of columns to restrict the result to. Return all columns if not specified.").hasArg(true).argName("col1,col2,...").valueSeparator(',').build() );
        options.addOption( Option.builder("f").required(false).longOpt("format").desc("Optional: The output format, csv(comma separated value) or tsv(tab separated value). tsv by default.").hasArg(true).argName("tsv/csv").build() );
        options.addOption( Option.builder("l").required(false).longOpt("limit").desc("Optional: The limit of records returned. Read all records if not specified.").hasArg(true).argName("number").build() );
        options.addOption( Option.builder("m").required(false).longOpt("min").desc("Optional: The minimum timestamp. NULL by default.").hasArg(true).argName("timestamp").build() );
        options.addOption( Option.builder("M").required(false).longOpt("MAX").desc("Optional: The maximum timestamp. NULL by default.").hasArg(true).argName("timestamp").build() );
        options.addOption( Option.builder("e").required(false).longOpt("engine").desc("Optional: The query engine, presto or hive. presto by default.").hasArg(true).argName("engine").build() );

        return options;
    }

    /**
     * Validate the arguments.
     *
     * @param options options to be validated
     * @param args arguments specified in the command line
     * @return CommandLine object contains option and arguments
     * @throws ParseException if any problem happened while parsing
     *
     */
    public CommandLine validateArgs(Options options, String[] args) throws ParseException {
        CommandLine cmd;

//        CommandLineParser parser = new DefaultParser();
        CommandLineParser parser = new ExactMatchParser();
        cmd = parser.parse( options, args );

        // validate the required arguments for database name and table name
        List<String> parsed_args = cmd.getArgList();
        logger.debug("required arguments: {}", parsed_args.toString());
        if (parsed_args.size() != 2) {
            // only two arguments are allowed, that is database name and table name
            throw new ParseException(Constants.MSG_MISSING_REQUIRED_ARGS);
        }

        // check engine option
        if (cmd.hasOption('e') || cmd.hasOption("engine")) {
            String engine = cmd.getOptionValue("e");
            logger.debug("engine: {}", engine);
            if (!engine.toLowerCase().equals(Constants.QUERY_ENGINE_PRESTO) && !engine.toLowerCase().equals(Constants.QUERY_ENGINE_HIVE)) {
                throw new ParseException(Constants.INVALID_ENGINE);
            }
        }
        // check format option
        if (cmd.hasOption('f') || cmd.hasOption("format")) {
            String format = cmd.getOptionValue("f");
            logger.debug("format: {}", format);
            if (!format.toLowerCase().equals(Constants.OUTPUT_FORMAT_TSV) && !format.toLowerCase().equals(Constants.OUTPUT_FORMAT_CSV)) {
                throw new ParseException(Constants.INVALID_FORMAT);
            }
        }
        return cmd;
    }

    /**
     * Produce an valid query statement by parsing the args.
     *
     * @param cmd CommandLine object contains option values and arguments
     * @return an valid query statement
     *
     */
    public String getSqlFromArgs(CommandLine cmd) {
        // options' default values
        String columns_value = "*";
        String limit_value = "";
        String min_value = "";
        String max_value = "";

        List<String> parsed_args = cmd.getArgList();
        String table_name = parsed_args.get(1);

        // check column option
        if (cmd.hasOption('c') || cmd.hasOption("column")) {
            columns_value = cmd.getOptionValue("c");
            logger.debug("columns: {}", columns_value);
        }

        String sql_st = "select " + columns_value + " from " + table_name + " where 1=1 ";

        // check min option
        if (cmd.hasOption('m') || cmd.hasOption("min")) {
            min_value = cmd.getOptionValue("m");
            logger.debug("min: {}", min_value);
            sql_st += " and time > " + min_value;
        }

        // check max option
        if (cmd.hasOption('M') || cmd.hasOption("MAX")) {
            max_value = cmd.getOptionValue("M");
            logger.debug("max: {}", max_value);
            sql_st += " and time < " + max_value;
        }

        // check limit option
        if (cmd.hasOption('l') || cmd.hasOption("limit")) {
            limit_value = cmd.getOptionValue("l");
            logger.debug("limit: {}", limit_value);
            sql_st += " limit " + limit_value;
        }
        return sql_st;
    }
}
