package org.chronos.chronodb.inmemory.provider;

import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.builder.database.ChronoDBBackendBuilder;
import org.chronos.chronodb.api.builder.database.spi.ChronoDBBackendProvider;
import org.chronos.chronodb.api.builder.database.spi.TestSuitePlugin;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.inmemory.InMemoryChronoDBConfiguration;
import org.chronos.chronodb.inmemory.builder.ChronoDBInMemoryBuilderImpl;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.common.configuration.ChronosConfigurationUtil;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class InMemoryChronoDBBackendProvider implements ChronoDBBackendProvider {

    private static final Set<String> SUPPORTED_BACKEND_NAMES = Collections.unmodifiableSet(Sets.newHashSet(InMemoryChronoDB.BACKEND_NAME, "memory", "in-memory"));

    private final TestSuitePlugin testSuitePlugin = new InMemoryChronoDBTestSuitePlugin();

    @Override
    public boolean matchesBackendName(final String backendName) {
        checkNotNull(backendName, "Precondition violation - argument 'bak' must not be NULL!");
        return SUPPORTED_BACKEND_NAMES.contains(backendName);
    }

    @Override
    public Class<InMemoryChronoDBConfiguration> getConfigurationClass() {
        return InMemoryChronoDBConfiguration.class;
    }

    @Override
    public ChronoDBInternal instantiateChronoDB(final Configuration configuration) {
        InMemoryChronoDBConfiguration config = ChronosConfigurationUtil.build(configuration, this.getConfigurationClass());
        return new InMemoryChronoDB(config);
    }

    @Override
    public String getBackendName() {
        return InMemoryChronoDB.BACKEND_NAME;
    }

    @Override
    public TestSuitePlugin getTestSuitePlugin() {
        return this.testSuitePlugin;
    }

    @Override
    public ChronoDBBackendBuilder createBuilder() {
        return new ChronoDBInMemoryBuilderImpl();
    }
}