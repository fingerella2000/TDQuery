/**
 * Copyright (c) 2019 Bryan Luo. All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License, as
 * published by the Free Software Foundation.
 *
 */
package com.td.toolkit.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;

/**
 * Provide methods to run SQL statement.
 *
 * @author  Bryan Luo
 * @version 1.0
 * @since   1.0
 *
 */
public class QueryExecutor {

    final static Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    private String sqlSt;
    private TDClient client;

    public void setCmd(CommandLine cmd) {
        this.cmd = cmd;
    }

    private CommandLine cmd;

    /**
     * Initialize the TDClient
     */
    public QueryExecutor() {
        Properties properties = new Properties();
        this.client = TDClient.newClient();

        // try using the property files where user can define TD API key and endpoint
        InputStream is = this.getClass().getResourceAsStream("/tdclient.properties");
        if (is != null ) {
            try {
                properties.load(is);
                this.client = TDClient.newBuilder().setProperties(properties).build();
                logger.debug("API key and endpoint are override by values in the property file.");
            } catch (IOException e) {
                logger.debug("tdclient.properties file is not set correctly, still use default td.conf configuration");
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    logger.error("tdclient.properties file is not closed correctly.", e);
                }
            }
        }
    }

    /**
     * Execute the SQL statement
     *
     * @return a map with only one entry which key is the row count and value is records in string
     * @throws InterruptedException
     * @throws JobFailedException
     */
    public Map runSql() throws InterruptedException, JobFailedException {
        // 1. generate the SQL statement using CommandLine object
        prepareSql();

        // 2. validate if the given database name and table name is valid
        validateDbAndTable();

        // 3. run the TD job
        String jobId = runTDJob();

        // 4. throw exception if no zero row returned
        long rows_returned = client.jobInfo(jobId).getNumRecords();
        if(rows_returned == 0) {
            throw new JobFailedException(Constants.JOB_FINISHED_UNSUCCEED_WITH_NO_RECORD_FOUND);
        }

        // 5. validate if the returned columns in compliance with pre-defined table schema
        String returned_columns = validateReturnColumns(jobId);

        // 6. process the result with the particular format
        String result = processJobResult(jobId);

        // 7. return the result as a map which key is row count and value is result content
        Map<Long, String> result_map = new HashMap<Long, String>();
        result_map.put(new Long(rows_returned), returned_columns + "\n" + result);
        return result_map;
    }

    /**
     * Produce an valid query statement
     *
     */
    private void prepareSql() {
        // options' default values
        String columns_value = "*";
        String limit_value = "";
        String min_value = "";
        String max_value = "";

        List<String> parsed_args = cmd.getArgList();
        String table_name = parsed_args.get(1);

        // check column option
        if (cmd.hasOption('c') || cmd.hasOption("column")) {
            columns_value = cmd.getOptionValue("c").toLowerCase().trim();
            logger.debug("columns: {}", columns_value);
        }

        this.sqlSt = "select " + columns_value + " from " + table_name + " where 1=1 ";

        // check min option
        if (cmd.hasOption('m') || cmd.hasOption("min")) {
            min_value = cmd.getOptionValue("m").trim();
            logger.debug("min: {}", min_value);
            if(!min_value.toLowerCase().equals("null")) {
                this.sqlSt += " and time >= " + min_value;
            }
        }

        // check max option
        if (cmd.hasOption('M') || cmd.hasOption("MAX")) {
            max_value = cmd.getOptionValue("M").trim();
            logger.debug("max: {}", max_value);
            if(!max_value.toLowerCase().equals("null")) {
                this.sqlSt += " and time <= " + max_value;
            }
        }

        // check limit option
        if (cmd.hasOption('l') || cmd.hasOption("limit")) {
            limit_value = cmd.getOptionValue("l").trim();
            logger.debug("limit: {}", limit_value);
            this.sqlSt += " limit " + limit_value;
        }
        logger.debug("sql statement: {}", this.sqlSt);
    }

    /**
     * Process the result with the particular format
     * @param jobId
     * @return
     */
    private String processJobResult(String jobId) {
        String result = "";
        String format_value = Constants.OUTPUT_FORMAT_TSV;

        // get format option
        if (cmd.hasOption('f') || cmd.hasOption("format")) {
            format_value = cmd.getOptionValue("f").trim();
        }

        // output query result in a specific format according to the format type
        if (format_value.toLowerCase().equals(Constants.OUTPUT_FORMAT_TSV)) {
            // Read the job results in TSV
            result = client.jobResult(jobId, TDResultFormat.TSV, new Function<InputStream, String>() {
                @Override
                public String apply(InputStream input) {
                    try {
                        String result = CharStreams.toString(new InputStreamReader(input));
                        return result;
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            });
        } else if (format_value.toLowerCase().equals(Constants.OUTPUT_FORMAT_CSV)) {
            // Read the job results in CSV
            result = client.jobResult(jobId, TDResultFormat.CSV, new Function<InputStream, String>() {
                @Override
                public String apply(InputStream input) {
                    try {
                        String result = CharStreams.toString(new InputStreamReader(input));
                        return result;
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            });
        }
        return result;
    }

    /**
     * Validate if the returned columns in compliance with pre-defined table schema
     * @param jobId
     * @return column name using the same format as the result content
     * @throws JobFailedException
     */
    private String validateReturnColumns(String jobId) throws JobFailedException{

        String format_value = Constants.OUTPUT_FORMAT_TSV;

        // get format option
        if (cmd.hasOption('f') || cmd.hasOption("format")) {
            format_value = cmd.getOptionValue("f").trim();
        }

        // get engine type from arguments
        String engine_value = Constants.QUERY_ENGINE_PRESTO;
        if (cmd.hasOption('e') || cmd.hasOption("engine")) {
            engine_value = cmd.getOptionValue("e").trim();
        }

        String columns_value = Constants.OUTPUT_COLUMN_ALL;

        // get column option
        if (cmd.hasOption('c') || cmd.hasOption("column")) {
            columns_value = cmd.getOptionValue("c").toLowerCase().trim();
        }

        String returned_columns = "";
        Optional<String> schema_returned = client.jobInfo(jobId).getResultSchema();

        if(schema_returned.isPresent()) {
            String schema = "{\"columns\":" + schema_returned.get() + "}";
            ObjectMapper mapper = new ObjectMapper();
            TDSchema schema_obj;
            try {
                schema_obj = mapper.readValue(schema , TDSchema.class);
            }
            catch (IOException e) {
                throw new JobFailedException("Result schema is not in correct JSON format.", e);
            }

            // check columns are returned
            boolean is_col_name_valid = false;

            if (columns_value.equals("*")){ // return all columns
                // 1. validate the column number is correct
                if (engine_value.equals(Constants.QUERY_ENGINE_PRESTO)) {
                    if (schema_obj.getColumns().length != Constants.TABLE_COLUMNS.size()) {
                        throw new JobFailedException(Constants.RETURNED_COLUMNS_NOT_MATCH_SCHEMA);
                    }
                }else if (engine_value.equals(Constants.QUERY_ENGINE_HIVE)){
                    // engine hive will return another virtual column named "v" with "map" column type, I guess it was used in map reduce
                    if (schema_obj.getColumns().length-1 != Constants.TABLE_COLUMNS.size()) {
                        throw new JobFailedException(Constants.RETURNED_COLUMNS_NOT_MATCH_SCHEMA);
                    }
                }
                // 2. validate the column name is correct
                boolean index_zero = true;
                for (String[] col : schema_obj.getColumns()){
                    if (format_value.toLowerCase().equals(Constants.OUTPUT_FORMAT_TSV)) {
                        if(index_zero) {
                            returned_columns = col[0];
                        }else {
                            returned_columns +=  "    " + col[0];
                        }
                    }else if (format_value.toLowerCase().equals(Constants.OUTPUT_FORMAT_CSV)) {
                        if(index_zero) {
                            returned_columns = col[0];
                        }else {
                            returned_columns +=  "," + col[0];
                        }
                    }

                    if (Constants.TABLE_COLUMNS.containsKey(col[0])){
                        is_col_name_valid = true;
                    }else {
                        is_col_name_valid = false;
                    }

                    // engine hive will return another virtual column named "v" with "map" column type, I guess it was used in map reduce
                    if (engine_value.equals(Constants.QUERY_ENGINE_HIVE)) {
                        if (col[0].toLowerCase().equals("v")) {
                            is_col_name_valid = false;
                        }
                    }
                    index_zero = false;
                }
                if(!is_col_name_valid) {
                    throw new JobFailedException(Constants.RETURNED_COLUMNS_NOT_MATCH_SCHEMA);
                }
            } else { // return columns specified in command line
                String[] cols = columns_value.split(",");
                // 1. validate the column number is correct
                if (schema_obj.getColumns().length != cols.length) {
                    throw new JobFailedException(Constants.RETURNED_COLUMNS_NOT_MATCH_SCHEMA);
                }
                // 2. validate the column name is correct
                boolean index_zero = true;
                for(String col : cols) {
                    col = col.trim();
                    if (format_value.toLowerCase().equals(Constants.OUTPUT_FORMAT_TSV)) {
                        if(index_zero) {
                            returned_columns = col;
                        }else {
                            returned_columns +=  "    " + col;
                        }
                    }else if (format_value.toLowerCase().equals(Constants.OUTPUT_FORMAT_CSV)) {
                        if(index_zero) {
                            returned_columns = col;
                        }else {
                            returned_columns +=  "," + col;
                        }
                    }
                    if (Constants.TABLE_COLUMNS.containsKey(col)){
                        is_col_name_valid = true;
                    }else {
                        is_col_name_valid = false;
                    }
                    index_zero = false;
                }
                if(!is_col_name_valid) {
                    throw new JobFailedException(Constants.RETURNED_COLUMNS_NOT_MATCH_SCHEMA);
                }
            }
        }else {
            throw new JobFailedException("Result schema is not presented.");
        }
        return returned_columns;
    }

    /**
     * Run Treasure Data Job
     *
     * @return job id after job finished
     * @throws JobFailedException if job failed with any reason
     * @throws InterruptedException if job was interrupted while running
     */
    private String runTDJob() throws JobFailedException, InterruptedException{

        List<String> parsed_args = cmd.getArgList();

        String db_name = parsed_args.get(0);

        String engine_value = Constants.QUERY_ENGINE_PRESTO;

        // get engine option
        if (cmd.hasOption('e') || cmd.hasOption("engine")) {
            engine_value = cmd.getOptionValue("e").trim();
        }

        String jobId = "";
        // initiate a job according to the engine type
        if (engine_value.toLowerCase().equals(Constants.QUERY_ENGINE_PRESTO)) {
            // Submit a new Presto query
            jobId = client.submit(TDJobRequest.newPrestoQuery(db_name, this.sqlSt));

        } else if (engine_value.toLowerCase().equals(Constants.QUERY_ENGINE_HIVE)) {
            // Submit a new Presto query
            jobId = client.submit(TDJobRequest.newHiveQuery(db_name, this.sqlSt));
        }


        // Wait until the query finishes
        ExponentialBackOff backOff = new ExponentialBackOff();
        TDJobSummary job = client.jobStatus(jobId);

        while (!job.getStatus().isFinished()) {
            Thread.sleep(backOff.nextWaitTimeMillis());
            job = client.jobStatus(jobId);
        }

        if (job.getStatus() != TDJob.Status.SUCCESS) {
            throw new JobFailedException(Constants.JOB_FINISHED_UNSUCCEED + job.getStatus());
        }

        return jobId;
    }

    /**
     * Validate if the dabase name and table name available in Treasure Data
     * @throws JobFailedException if dabase name or table name is invalid
     */
    private void validateDbAndTable() throws JobFailedException{
        List<String> parsed_args = cmd.getArgList();

        String db_name = parsed_args.get(0).trim();
        String table_name = parsed_args.get(1).trim();

        List<String> db_names = client.listDatabaseNames();
        // databaseName is not valid
        if (!db_names.contains(db_name)) {
            throw new JobFailedException(Constants.INVALID_DATABASE + " Available database are: " + db_names);
        }
        List<TDTable> table_names = client.listTables(db_name);
        boolean is_table_name_valid = false;
        // tableName is not valid
        for(TDTable table : table_names) {
            if (table.getName().equals(table_name)) {
                is_table_name_valid = true;
            }
        }
        if (!is_table_name_valid) {
            throw new JobFailedException(Constants.INVALID_TABLE + " Available tables are: " + table_names);
        }
    }

    /**
     * Closed the connection to Treasure Data Server
     */
    public void terminate() {
        if(client != null) {
            client.close();
        }
    }
}
