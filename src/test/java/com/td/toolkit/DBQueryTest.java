package com.td.toolkit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Unit test for DBQuery.
 */
public class DBQueryTest
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldReturnZero()
    {
        DBQuery querier = new DBQuery();
        assertEquals(0, querier.runQuery() );
    }
}
