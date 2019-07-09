package com.td.toolkit.utils;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class QueryExecutor {

    final static Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

    public QueryExecutor() {

    }

    /**
     *
     * @param database database name
     * @param engine engine type
     * @param format output format
     * @param sql sql statement
     * @return
     */
    public String execute(String database, String engine, String format, String sql) {
        Properties properties = new Properties();
        TDClient client;
        // Retrieve resource
        InputStream is = this.getClass().getResourceAsStream("/tdclient.properties");
        try {
            properties.load(is);
            // This overrides the default configuration parameters with the given Properties
            client = TDClient.newBuilder().setProperties(properties).build();
        } catch (IOException e) {
            logger.debug("TD property file not not found, use default td.conf configuration");
            client = TDClient.newClient();
        }

        String result = "";
        try {
            String jobId = "";
            if (engine.toLowerCase().equals(Constants.QUERY_ENGINE_PRESTO)) {
                // Submit a new Presto query
                jobId = client.submit(TDJobRequest.newPrestoQuery("bryandb", "select * from orders limit 10"));

            } else if (engine.toLowerCase().equals(Constants.QUERY_ENGINE_HIVE)) {
                // Submit a new Presto query
                jobId = client.submit(TDJobRequest.newHiveQuery("bryandb", "select * from orders limit 10"));
            }

            // Wait until the query finishes
            ExponentialBackOff backOff = new ExponentialBackOff();
            TDJobSummary job = client.jobStatus(jobId);
            while (!job.getStatus().isFinished()) {
                Thread.sleep(backOff.nextWaitTimeMillis());
                job = client.jobStatus(jobId);
            }

            if (format.toLowerCase().equals(Constants.OUTPUT_FORMAT_TSV)) {
                // Read the job results in TSV
                result = client.jobResult(jobId, TDResultFormat.TSV, new Function<InputStream, String>() {
                    @Override
                    public String apply(InputStream input) {
                        try {
                            String result = CharStreams.toString(new InputStreamReader(input));
                            logger.debug(result);
                            return result;
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
            } else if (format.toLowerCase().equals(Constants.OUTPUT_FORMAT_CSV)) {
                // Read the job results in CSV
                result = client.jobResult(jobId, TDResultFormat.CSV, new Function<InputStream, String>() {
                    @Override
                    public String apply(InputStream input) {
                        try {
                            String result = CharStreams.toString(new InputStreamReader(input));
                            logger.debug(result);
                            return result;
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });

            }
        }catch (Exception e) {
            logger.error("", e);
        }
        finally {
            client.close();
        }
        return result;
    }
}
