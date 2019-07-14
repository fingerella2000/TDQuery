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
 * Unit test for required arguments.
 */
public class TestRequiredArgs
{

    final static Logger logger = LoggerFactory.getLogger(TestRequiredArgs.class);

    private DBQuery query;

    @Before
    public void setUp() {
        query = new DBQuery();
    }

    @After
    public void tearDown() {
        query.end();
    }

    @Rule
    public ExpectedException parseExceptionRule = ExpectedException.none();


    @Test
    public void testEmptyDbAndTable() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.MSG_MISSING_REQUIRED_ARGS);
        String[] args = {};
        query.parseArgs(args);
    }

    @Test
    public void testEmptyDbAndValidTable() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.MSG_MISSING_REQUIRED_ARGS);
        String[] args = {"orders"};
        query.parseArgs(args);
    }

    @Test
    public void testEmptyDbAndInValidTable() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.MSG_MISSING_REQUIRED_ARGS);
        String[] args = {"table_not_existed"};
        query.parseArgs(args);
    }

    @Test
    public void testValidDbAndEmptyTable() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.MSG_MISSING_REQUIRED_ARGS);
        String[] args = {"bryandb"};
        query.parseArgs(args);
    }

    @Test
    public void testValidDbAndValidTable() throws ParseException, JobFailedException, InterruptedException
    {
        String[] args = {"bryandb","orders"};
        Map result = query.run(query.parseArgs(args));
        Map.Entry<Long, String> entry = (Map.Entry<Long, String>) result.entrySet().iterator().next();
        Assert.assertTrue(entry.getKey() > 0);
    }

    @Test
    public void testValidDbAndInvalidTable() throws ParseException, JobFailedException, InterruptedException
    {
        parseExceptionRule.expect(JobFailedException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_TABLE);

        String[] args = {"bryandb","table_not_existed"};
        query.run(query.parseArgs(args));
    }

    @Test
    public void testInvalidDbAndEmptyTable() throws ParseException
    {
        parseExceptionRule.expect(ParseException.class);
        parseExceptionRule.expectMessage(Constants.MSG_MISSING_REQUIRED_ARGS);
        String[] args = {"db_not_existed"};
        query.parseArgs(args);
    }

    @Test
    public void testInvalidDbAndValidTable() throws ParseException, JobFailedException, InterruptedException
    {
        parseExceptionRule.expect(JobFailedException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_DATABASE);

        String[] args = {"db_not_existed","orders"};
        query.run(query.parseArgs(args));
    }

    @Test
    public void testInvalidDbAndInalidTable() throws ParseException, JobFailedException, InterruptedException
    {
        parseExceptionRule.expect(JobFailedException.class);
        parseExceptionRule.expectMessage(Constants.INVALID_DATABASE);

        String[] args = {"db_not_existed","table_not_existed"};
        query.run(query.parseArgs(args));
    }
}
