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
 * Unit test for optional arguments.
 */
public class TestOptionalArgs {

    final static Logger logger = LoggerFactory.getLogger(TestRequiredArgs.class);

    @Rule
    public ExpectedException parseExceptionRule = ExpectedException.none();

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
//    4
    public void testIgnoreAll() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
//    5
    public void testValidMinMaxWithPrestoTsvLimitOneRow() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m", "1041778800", "-M", "1117465200",  "-e", "presto", "-f", "tsv", "-l", "1", "bryandb", "orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 1);
    }

    @Test
//    6
    public void testInvalidMinMaxWithHiveCsvLimitXRow() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP);
        String[] args = {"-m", "invalid", "-M", "invalid", "-e", "hive", "-f", "csv", "-l", "9", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    7
    public void testInvalidEngineFormatLimitMinGTMax() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_ENGINE);
        String[] args = {"-m", "1117465200", "-M", "1041778800", "-e", "invalid", "-f", "invalid", "-l", "\"-1\"", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    8
    public void testInvalidFormatWithOneColumnMaxHive() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_FORMAT);
        String[] args = {"-c", "ordernumber", "-M", "1117465200", "-e", "hive", "-f", "invalid", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    9
    public void testInvalidEngineWithOneColumnMinCsvLimitOne() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_ENGINE);
        String[] args = {"-c", "ordernumber", "-m", "1041778800", "-e", "invalid", "-f", "csv", "-l", "1", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    10
    public void testInvalidMinWithOneColumnMaxLTMinTsvLimitXRow() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP);
        String[] args = {"-c", "ordernumber", "-m", "invalid", "-M", "1117465200", "-l", "9", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    11
    public void testInvalidMaxLimitWithOneColumnMinGTMaxPresto() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP);
        String[] args = {"-c", "ordernumber", "-m", "1117465200", "-M", "invalid", "-e", "presto", "-l", "\"-9\"", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    12
    public void testInValidOneColumnMaxEngineWithTsv() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_ENGINE);
        String[] args = {"-c", "invalid", "-M", "invalid", "-e", "invalid", "-f", "tsv", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    13
    public void testInvalidOneColumnWithMaxLTMinHiveLimitOne() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP_RANGE);
        String[] args = {"-c", "invalid", "-m", "1117465200", "-M", "1041778800", "-e", "hive", "-l", "1", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    14
    public void testInvalidOneColumnMinFormatWithPrestoLimitXRow() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_FORMAT);
        String[] args = {"-c", "invalid", "-m", "invalid", "-f", "invalid", "-e", "presto", "-l", "9", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    15
    public void testInvalidOneColumnLimitWithMinGTMaxCsv() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP_RANGE);
        String[] args = {"-c", "invalid", "-l", "\"-9\"", "-m", "1117465200", "-M", "1041778800", "-f", "csv", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    16
    public void testMultiColumnPrestoCsv() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-c", "ordernumber,status", "-e", "presto", "-f", "csv", "bryandb", "orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
//    17
    public void testInvalidFormatMaxWithMultiColumnMinLimitOne() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_FORMAT);
        String[] args = {"-c", "ordernumber,status", "-m", "1117465200", "-l", "1", "-M", "invalid", "-f", "invalid", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    18
    public void testInvalidMinEngineWithMultiColumnMaxLimitXRow() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_ENGINE);
        String[] args = {"-c", "ordernumber,status", "-m", "invalid", "-e", "invalid", "-M", "1117465200", "-l", "9", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    19
    public void testInvalidLimitWithMultiColumnHiveTsv() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_LIMIT);
        String[] args = {"-c", "ordernumber,status",  "-l", "\"-1\"", "-e", "hive", "-f", "tsv", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    20
    public void testInvalidMutipleColumnWithNullMinMax() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c", "invalid1,invalid2",  "-m", "null", "-M", "null", "bryandb", "orders"};
        query.run(query.parseArgs(args));
    }


    @Test
//    21
    public void testInvalidMutipleColumnWithNullMinMaxPrestoTsvLimitOne() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c", "invalid1,invalid2",  "-m", "null", "-M", "null","-e","presto","-f","tsv","-l","1", "bryandb", "orders"};
        query.run(query.parseArgs(args));
    }

    @Test
//    22
    public void testInvalidMutipleColumnWithNullMinMaxHiveCsvLimitXRow() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c", "invalid1,invalid2",  "-m", "null", "-M", "null","-e","hive","-f","csv","-l","9", "bryandb", "orders"};
        query.run(query.parseArgs(args));
    }

    @Test
//    23
    public void testInvalidMutipleColumnEngineFormatLimitWithNullMinMax() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_ENGINE);
        String[] args = {"-c", "invalid1,invalid2","-e","invalid","-f","invalid", "-m", "null", "-M", "null","bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    24
    public void testMixedColumnWithNullMaxTsvLimitXRow() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c", "ordernumber,invalid2", "-M", "null","-f","tsv","-l","9", "bryandb", "orders"};
        query.run(query.parseArgs(args));

    }

    @Test
//    25
    public void testMixedColumnLimitWithMinPresto() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_LIMIT);
        String[] args = {"-c", "ordernumber,invalid2", "-l", "\"-9\"","-m","1041778800","-e","presto", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    26
    public void testMixedColumnMinFormatWithMaxHive() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_FORMAT);
        String[] args = {"-c", "ordernumber,invalid2", "-m","invalid","-f","invalid","-M","1117465200","-e","hive", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    27
    public void testMixedColumnMaxEngineWithMinGTMaxCsvLimitOneRow() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_ENGINE);
        String[] args = {"-c", "ordernumber,invalid2","-M","1041778800","-e","invalid","-m","1117465200","-f","csv","-l","1", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    28
    public void testValidMinNullMax() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m", "1041778800", "-M", "null", "bryandb", "orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
//    29
    public void testInvalidMinWithOneColumnNullMax() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP);
        String[] args = {"-c","ordernumber","-m", "invalid","-M","null", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    30
    public void testInvalidOneColumnWithNullMaxMinGTMaxLimitXRow() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c","invalid","-M","null","-m","1117465200","-l","9","bryandb", "orders"};
        query.run(query.parseArgs(args));
    }

    @Test
//    31
    public void testMultiColumnNullMin() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-c","ordernumber,status","-m", "null", "bryandb", "orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
//    32
    public void testInvalidMultipleColumnWithLimitOne() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c","invalid1,invalid2","-l","1", "bryandb", "orders"};
        query.run(query.parseArgs(args));
    }

    @Test
//    33
    public void testInvalidMultipleColumnWithMinMaxLimitXRow() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c","invalid1,invalid2","-m","1041778800","-M","1117465200","-l","9","bryandb", "orders"};
        query.run(query.parseArgs(args));
    }

    @Test
//    34
    public void testInvalidMultipleColumnMinMaxWithLimitOne() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP);
        String[] args = {"-c","invalid1,invalid2","-m", "invalid","-M","invalid","-l","1","bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    35
    public void testInvalidMultipleColumnWithMinGTMax() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP_RANGE);
        String[] args = {"-c","invalid1,invalid2","-m", "1117465200","-M","1041778800","bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    36
    public void testMixedColumnMaxLTMinWithNullMin() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c", "ordernumber,invalid2", "-m", "null","-M","1041778800", "bryandb", "orders"};
        query.run(query.parseArgs(args));
    }

    @Test
//    37
    public void testValidMaxWithNullMin() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m", "null", "-M","1117465200","bryandb", "orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
//    38
    public void testInvalidMaxWithOneColumnNullMin() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP);
        String[] args = {"-c","ordernumber","-m", "null","-M","invalid","bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    39
    public void testInvalidOneColumnLimit() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_LIMIT);
        String[] args = {"-c","invalid","-l","invalid","bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
//    40
    public void testInvalidOneColumnWithNullMin() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c", "invalid2", "-m", "null", "bryandb", "orders"};
        query.run(query.parseArgs(args));
    }

    @Test
//    41
    public void testInvalidMultipleColumnWithNullMax() throws ParseException, JobFailedException, InterruptedException
    {
        jobExceptionRule.expect(JobFailedException.class);
        jobExceptionRule.expectMessage(Constants.JOB_FINISHED_UNSUCCEED);
        String[] args = {"-c", "invalid1,invalid2", "-M", "null", "bryandb", "orders"};
        query.run(query.parseArgs(args));
    }

}

