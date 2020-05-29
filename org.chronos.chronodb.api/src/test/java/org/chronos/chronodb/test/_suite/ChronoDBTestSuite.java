package org.chronos.chronodb.test._suite;

import org.chronos.common.test.junit.ExcludeCategories;
import org.chronos.common.test.junit.PackageSuite;
import org.chronos.common.test.junit.SuitePackages;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.junit.categories.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@Category(Suite.class)
@RunWith(PackageSuite.class)
@SuitePackages("org.chronos.chronodb.test")
@ExcludeCategories({PerformanceTest.class, SlowTest.class})
public class ChronoDBTestSuite {

}
