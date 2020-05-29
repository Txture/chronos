package org.chronos.chronodb.api.builder.database.spi;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.builder.database.ChronoDBBackendBuilder;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBInternal;

public interface ChronoDBBackendProvider {

    public boolean matchesBackendName(String backendName);

    public Class<? extends ChronoDBConfiguration> getConfigurationClass();

    public ChronoDBInternal instantiateChronoDB(Configuration configuration);

    public String getBackendName();

    public TestSuitePlugin getTestSuitePlugin();

    public ChronoDBBackendBuilder createBuilder();

}
