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
 * Test columns return from result is correct and meet the table schema
 */
public class TestReturnedColumns {

    final static Logger logger = LoggerFactory.getLogger(TestReturnedColumns.class);

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
    public void testWithoutColumnOption() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 1);
    }

    @Test
    public void testAllColumn() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-c","*","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 1);
    }

    @Test
    public void testOneColumn() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-c","ordernumber","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 1);
    }

    @Test
    public void testMultipleColumn() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-c","ordernumber,status","bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 1);
    }

    @Test
    public void testOneInvalidColumn() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c","invalid","bryandb","orders"};
        query.run(query.parseArgs(args));
    }

    @Test
    public void testMultipleInvalidColumn() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c","invalid1,invalid2","bryandb","orders"};
        query.run(query.parseArgs(args));
    }

    @Test
    public void testOneValidColumnWithMultipleInvalidColumn() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c","ordernumber,invalid1,invalid2","bryandb","orders"};
        query.run(query.parseArgs(args));
    }

    @Test
    public void testMultipleValidColumnWithMultipleInvalidColumn() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c","ordernumber,status,invalid1,invalid2","bryandb","orders"};
        query.run(query.parseArgs(args));
    }

    @Test
    public void testMultipleValidColumnWithOneInvalidColumn() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c","ordernumber,status,invalid","bryandb","orders"};
        query.run(query.parseArgs(args));
    }
}
