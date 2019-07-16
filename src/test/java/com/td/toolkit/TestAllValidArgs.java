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
 * Unit test for all valid arguments.
 */
public class TestAllValidArgs {

    final static Logger logger = LoggerFactory.getLogger(TestAllValidArgs.class);

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
    public void testIgnoreAll() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testMinMaxPrestoTsvLimitXRow() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-m","1041778800","-M","1117465200","-e","presto","-f","tsv","-l","9"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testOneColumnMaxTsv() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-c","ordernumber","-M","1117465200","-f","tsv"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testOneColumnMinPrestoLimitXRow() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-c"," ordernumber ","-m"," 1041778800 ","-e"," presto ","-l"," 9 "};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testMutipleColumnHiveCsvLimitXRow() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-c","ordernumber,status","-e","hive","-f","csv","-l","9"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testMutipleColumnMinMaxCsv() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-c"," ordernumber, status ","-m"," 1041778800 ","-M"," 1117465200 ","-f"," csv "};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testMinMaxHive() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-m","1041778800","-M","1117465200","-e","hive"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testOneColumnPrestoCsv() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-c","ordernumber","-e","presto","-f","csv"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testMultipleColumnTsvLimitXRow() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-c"," ordernumber , status ","-f"," tsv ","-l"," 9 "};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testMultiplePresto() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-c","ordernumber,status","-e","presto"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testHiveTsv() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-f","tsv","-e","hive"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testOneColumnHive() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-c","ordernumber","-e","hive"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testCsv() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders","-f","csv"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

}

