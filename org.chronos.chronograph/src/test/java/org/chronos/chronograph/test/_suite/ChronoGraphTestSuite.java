package org.chronos.chronograph.test._suite;

import org.chronos.chronodb.test._suite.ChronoDBTestSuite;
import org.chronos.common.test.junit.ExcludeCategories;
import org.chronos.common.test.junit.PackageSuite;
import org.chronos.common.test.junit.SuiteIncludes;
import org.chronos.common.test.junit.SuitePackages;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.junit.categories.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@Category(Suite.class)
@RunWith(PackageSuite.class)
@SuitePackages("org.chronos.chronograph.test.cases")
@SuiteIncludes(ChronoDBTestSuite.class)
@ExcludeCategories({PerformanceTest.class, SlowTest.class})
public class ChronoGraphTestSuite {

}