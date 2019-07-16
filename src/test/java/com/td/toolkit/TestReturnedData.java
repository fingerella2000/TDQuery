package com.td.toolkit;

import com.td.toolkit.utils.Constants;
import com.td.toolkit.utils.JobFailedException;
import org.apache.commons.cli.ParseException;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Test row number returned from query is correct
 */
public class TestReturnedData {

    final static Logger logger = LoggerFactory.getLogger(TestReturnedData.class);

    @Rule
    public ExpectedException jobExceptionRule = ExpectedException.none();

    private static DBQuery query;

    @BeforeClass
    public static void setUp() {
        query = new DBQuery();
    }

    @AfterClass
    public static void tearDown() {
        query.end();
    }

    @Test
    public void testLimitOneRow() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-l","1","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 1);
    }

    @Test
    public void testWithoutLimit() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 326);
    }

    @Test
    public void testWithSpecifiedTime() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m","1101222000","-M","1101222000","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 4);
    }

    @Test
    public void testSpecifiedTimeLimitMoreThanResult() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m","1101222000","-M","1101222000","-l","5","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 4);
    }

    @Test
    public void testSpecifiedTimeLimitLessThanResult() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m","1101222000","-M","1101222000","-l","3","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 3);
    }

    @Test
    public void testReturnZeroRow() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED_WITH_NO_RECORD_FOUND);
        String[] args = {"-m","1290524400","-M","1290524400","bryandb","orders"};
        query.run(query.parseArgs(args));
    }

    @Test
    public void testTimeRange() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m","1101654000","-M","1104850800","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 16);
    }

    @Test
    public void testGreaterThanTime() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m","1114873200","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 15);
    }

    @Test
    public void testLessThanTime() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-M","1047913200","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 11);
    }
}
