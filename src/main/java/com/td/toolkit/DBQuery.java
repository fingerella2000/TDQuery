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
import com.td.toolkit.utils.JobFailedException;
import com.td.toolkit.utils.QueryExecutor;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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

        // initialize the constant variables
        Constants constants = new Constants();
        constants.loadTableColumns();

        ArgsHandler args_handler = new ArgsHandler(args);
        HelpFormatter formatter = new HelpFormatter();

        if (args.length == 0) { // print help message if the command does not have any arguments
            formatter.printHelp( Constants.MSG_HELP, args_handler.getOptions() );
            System.exit(0);
        }else if (args.length == 1 && (args[0].trim().equals("-h") || args[0].trim().equals("--help"))) { // print help message if the arguments only have "-h" and "--help" option
            formatter.printHelp( Constants.MSG_HELP, args_handler.getOptions() );
            System.exit(0);
        }

        try {
            // using handler to parse arguments
            args_handler.parseArgs();

            // execute the query using QueryExecutor object
            QueryExecutor executor = new QueryExecutor();
            Map result = executor.runSql(args_handler.getCmd(), args);

            Map.Entry<Long,String> entry = (Map.Entry<Long,String>)result.entrySet().iterator().next();
            System.out.println(entry.getValue());
            System.exit(0);
        }catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(Constants.MSG_HELP, args_handler.getOptions() );
            logger.debug( e.getMessage(), e);
            System.exit(0);
        }catch (JobFailedException e) {
            System.out.println(e.getMessage());
            logger.debug( e.getMessage(), e);
            System.exit(-1);
        }catch (InterruptedException e) {
            System.out.println("Query job is interrupted without succeeding, there could be an error on the Treasure Data server, please contact support with the following  message.\n" + e.getMessage());
            logger.debug( e.getMessage(), e);
            System.exit(-1);
        }
    }
}
