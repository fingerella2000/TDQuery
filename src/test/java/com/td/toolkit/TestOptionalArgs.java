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

    private DBQuery query;

    @Before
    public void setUp() {
        query = new DBQuery();
    }

    @After
    public void tearDown() {
        query.end();
    }

    @Ignore
    @Test
    public void testIgnoreAll() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testValidMinMaxWithPrestoTsvLimitOneRow() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"-m", "1041778800", "-M", "1117465200",  "-e", "presto", "-f", "tsv", "-l", "1", "bryandb", "orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() == 1);
    }

    @Test
    public void testInvalidMinMaxWithHiveCsvLimitXRow() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP);
        String[] args = {"-m", "invalid", "-M", "invalid", "-e", "hive", "-f", "csv", "-l", "9", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
    public void testMinGreaterThanMaxWithInvalidEngineFormatLimit() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_ENGINE);
        String[] args = {"-m", "1117465200", "-M", "1041778800", "-e", "invalid", "-f", "invalid", "-l", "invalid", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
    public void testValidOneColumnMaxHiveWithInvalidFormat() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_FORMAT);
        String[] args = {"-c", "ordernumber", "-M", "1117465200", "-e", "hive", "-f", "invalid", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
    public void testValidOneColumnMinWithInvalidEngineCsvLimitOne() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_ENGINE);
        String[] args = {"-c", "ordernumber", "-m", "1041778800", "-e", "invalid", "-f", "csv", "-l", "1", "bryandb", "orders"};
        query.parseArgs(args);
    }

    @Test
    public void testValidOneColumnMinGreaterThanMaxWithInvalidMaxPrestoLimit() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TIME_STAMP);
        String[] args = {"-c", "ordernumber", "-m", "1117465200", "-M", "invalid", "-e", "presto", "-l", "invalid", "bryandb", "orders"};
        query.parseArgs(args);
    }
}
