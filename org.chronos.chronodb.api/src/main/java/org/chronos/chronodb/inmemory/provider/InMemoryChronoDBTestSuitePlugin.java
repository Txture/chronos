package org.chronos.chronodb.inmemory.provider;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.chronos.chronodb.api.builder.database.spi.TestSuitePlugin;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;

import java.io.File;
import java.lang.reflect.Method;

public class InMemoryChronoDBTestSuitePlugin implements TestSuitePlugin {

    @Override
    public Configuration createBasicTestConfiguration(final Method testMethod, final File testDirectory) {
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty(ChronoDBConfiguration.STORAGE_BACKEND, InMemoryChronoDB.BACKEND_NAME);
        return configuration;
    }

    @Override
    public void onBeforeTest(final Class<?> testClass, final Method testMethod, final File testDirectory) {
        // nothing to do
    }

    @Override
    public void onAfterTest(final Class<?> testClass, final Method testMethod, final File testDirectory) {
        // nothing to do
    }
}
