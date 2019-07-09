/**
 * Copyright (c) 2019 Bryan Luo. All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License, as
 * published by the Free Software Foundation.
 *
 */
package com.td.toolkit;

import com.td.toolkit.utils.ArgsHandler;
import com.td.toolkit.utils.Constants;
import com.td.toolkit.utils.QueryExecutor;
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

        ArgsHandler args_handler = new ArgsHandler();
        Options options = args_handler.buildOptions(args);

        HelpFormatter formatter = new HelpFormatter();
        if (args.length == 0) { // print help message if the command does not have any arguments
            formatter.printHelp( Constants.MSG_HELP, options );
            System.exit(0);
        }else if (args.length == 1 && (args[1].trim().equals("-h") || args[1].trim().equals("--help"))) { // print help message if the arguments only have "-h" and "--help" option
            formatter.printHelp( Constants.MSG_HELP, options );
            System.exit(0);
        }

        try {
            // using validated arguments and options to build a CommandLine object
            CommandLine cmd = args_handler.validateArgs(options, args);

            // generate sql statement from the CommandLine object
            String sql_st = args_handler.getSqlFromArgs(cmd);

            List<String> parsed_args = cmd.getArgList();

            String db_name = parsed_args.get(0);
            String engine_value = Constants.QUERY_ENGINE_PRESTO;
            String format_value = Constants.OUTPUT_FORMAT_TSV;

            // check engine option
            if (cmd.hasOption('e') || cmd.hasOption("engine")) {
                engine_value = cmd.getOptionValue("e");
            }
            // check format option
            if (cmd.hasOption('f') || cmd.hasOption("format")) {
                format_value = cmd.getOptionValue("f");
            }

            // execute the query using QueryExecutor object
            QueryExecutor executor = new QueryExecutor();
            String result = executor.execute(db_name, engine_value, format_value, sql_st);
            System.out.println(result);
            System.exit(0);
        }catch (ParseException e) {
            formatter.printHelp( Constants.MSG_HELP, options );
            logger.error( e.getMessage() );
            logger.error("An exception occurred!", e);
            System.exit(-1);
        }
    }
}
