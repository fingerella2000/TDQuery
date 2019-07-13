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

public class QueryExecutor {

    final static Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    private String sqlSt;

    public QueryExecutor() { }


    /**
     * Produce an valid query statement
     *
     */
    public void prepareSql(CommandLine cmd) {
        // options' default values
        String columns_value = "*";
        String limit_value = "";
        String min_value = "";
        String max_value = "";

        List<String> parsed_args = cmd.getArgList();
        String table_name = parsed_args.get(1);

        // check column option
        if (cmd.hasOption('c') || cmd.hasOption("column")) {
            columns_value = cmd.getOptionValue("c").toLowerCase();
            logger.debug("columns: {}", columns_value);
        }

        this.sqlSt = "select " + columns_value + " from " + table_name + " where 1=1 ";

        // check min option
        if (cmd.hasOption('m') || cmd.hasOption("min")) {
            min_value = cmd.getOptionValue("m");
            logger.debug("min: {}", min_value);
            this.sqlSt += " and time >= " + min_value;
        }

        // check max option
        if (cmd.hasOption('M') || cmd.hasOption("MAX")) {
            max_value = cmd.getOptionValue("M");
            logger.debug("max: {}", max_value);
            this.sqlSt += " and time <= " + max_value;
        }

        this.sqlSt += " order by time desc";
        // check limit option
        if (cmd.hasOption('l') || cmd.hasOption("limit")) {
            limit_value = cmd.getOptionValue("l");
            logger.debug("limit: {}", limit_value);
            this.sqlSt += " limit " + limit_value;
        }
    }

    /**
     *
     * @param cmd CommandLine ojbect
     * @param args arguments passed from CLI
     * @return a map with only one entry which key is the row count and value is records in string
     * @throws InterruptedException
     * @throws JobFailedException
     */
    public Map runSql(CommandLine cmd, String[] args) throws InterruptedException, JobFailedException {
        Map<Long, String> result_map = new HashMap<Long, String>();
        prepareSql(cmd);

        List<String> parsed_args = cmd.getArgList();

        String db_name = parsed_args.get(0);
        String table_name = parsed_args.get(1);

        Properties properties = new Properties();
        TDClient client = TDClient.newClient();

        // try using the property files where user can define TD API key and endpoint
        InputStream is = this.getClass().getResourceAsStream("/tdclient.properties");
        if (is != null ) {
            try {
                properties.load(is);
                client = TDClient.newBuilder().setProperties(properties).build();
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

        List<String> db_names = client.listDatabaseNames();
        // databaseName is not valid
        if (!db_names.contains(db_name)) {
            throw new JobFailedException("Invalid database name. Available database are: " + db_names);
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
            throw new JobFailedException("Invalid table name. Available tables are: " + table_names);
        }

        String engine_value = Constants.QUERY_ENGINE_PRESTO;
        String format_value = Constants.OUTPUT_FORMAT_TSV;
        String columns_value = "*";
        String result = "";

        // get engine option
        if (cmd.hasOption('e') || cmd.hasOption("engine")) {
            engine_value = cmd.getOptionValue("e");
        }
        // get format option
        if (cmd.hasOption('f') || cmd.hasOption("format")) {
            format_value = cmd.getOptionValue("f");
        }
        // get column option
        if (cmd.hasOption('c') || cmd.hasOption("column")) {
            columns_value = cmd.getOptionValue("c").toLowerCase();
            columns_value.trim();
        }

        try {
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
                throw new JobFailedException("Query job finished with an unsuccessful status.");
            }

            long rows_returned = client.jobInfo(jobId).getNumRecords();
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
                        index_zero = false;
                    }
                    if(!is_col_name_valid) {
                        throw new JobFailedException("Returned columns does not match the schema.");
                    }
                } else { // return columns specified in command line
                    String[] cols = columns_value.split(",");
                    boolean index_zero = true;
                    for(String col : cols) {
                        col.trim();
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
                        throw new JobFailedException("Returned columns does not match the schema.");
                    }
                }
            }else {
                throw new JobFailedException("Result schema is not presented.");
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
            result_map.put(new Long(rows_returned), returned_columns + "\n" + result);
        }
        finally {
            client.close();
        }
        return result_map;
    }
}
