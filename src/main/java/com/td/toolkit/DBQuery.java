/**
 * Copyright (c) 2019 Bryan Luo. All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License, as
 * published by the Free Software Foundation.
 *
 */
package com.td.toolkit;

import com.td.toolkit.utils.*;
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

    private QueryExecutor executor;

    public DBQuery() {
        executor = new QueryExecutor();
    }

    public static void main(String[] args) {
        DBQuery query = new DBQuery();
        try {
            query.run(query.parseArgs(args));
            query.end();
        }catch (ParseException e) {
            System.out.println(e.getMessage());
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

    /**
     * Parsing the arguments passed from CLI
     *
     * @param args CLI arguments
     * @return CommandLine object
     * @throws ParseException if error happend while parsing
     */
    public CommandLine parseArgs(String[] args) throws ParseException {
        ArgsHandler args_handler = new ArgsHandler(args);
        // using handler to parse arguments
        CommandLine cmd = args_handler.parseArgs();
        return cmd;
    }

    /**
     * Run a command
     * @param cmd the CommandLine object
     * @return a map with only one entry which key is the row count and value is records in string
     * @throws JobFailedException
     * @throws InterruptedException
     */
    public Map run(CommandLine cmd) throws JobFailedException, InterruptedException{
        // initialize the executor with CommandLine object
        executor.setCmd(cmd);

        // execute the query using QueryExecutor object
        Map result = executor.runSql();

        // print out the result
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        System.out.println(entry.getValue());
        return result;
    }

    // End up the query process
    public void end() {
        executor.terminate();
    }
}
