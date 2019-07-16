package com.td.toolkit;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({TestAllValidArgs.class, TestOptionalArgs.class, TestRequiredArgs.class, TestReturnedColumns.class, TestReturnedData.class})
public class SanityTestSuite {

}
