package com.td.toolkit.utils;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
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
    public String execute(String database, String engine, String format, String sql) throws InterruptedException, Exception {
        Properties properties = new Properties();
        TDClient client = TDClient.newClient();
        try { // try using the property files where user can define TD API key and endpoint
            InputStream is = this.getClass().getResourceAsStream("/tdclient.properties");
            properties.load(is);
            client = TDClient.newBuilder().setProperties(properties).build();
            logger.debug("API key and endpoint are override by values in the property file.");
        } catch (IOException e) {
            logger.debug("tdclient.properties file not not found, still use default td.conf configuration");
        }

        String result = "";
        try {
            String jobId = "";
            // initiate a job according to the engine type
            if (engine.toLowerCase().equals(Constants.QUERY_ENGINE_PRESTO)) {
                // Submit a new Presto query
                jobId = client.submit(TDJobRequest.newPrestoQuery(database, sql));

            } else if (engine.toLowerCase().equals(Constants.QUERY_ENGINE_HIVE)) {
                // Submit a new Presto query
                jobId = client.submit(TDJobRequest.newHiveQuery(database, sql));
            }

            // Wait until the query finishes
            ExponentialBackOff backOff = new ExponentialBackOff();
            TDJobSummary job = client.jobStatus(jobId);

            while (!job.getStatus().isFinished()) {
                Thread.sleep(backOff.nextWaitTimeMillis());
                job = client.jobStatus(jobId);
            }

            if (job.getStatus() != TDJob.Status.SUCCESS) {
                throw new Exception("Query job finished with an unsuccessful statue.");
            }
            // output query result in a specific format according to the format type
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
        }
        finally {
            client.close();
        }
        return result;
    }
}
