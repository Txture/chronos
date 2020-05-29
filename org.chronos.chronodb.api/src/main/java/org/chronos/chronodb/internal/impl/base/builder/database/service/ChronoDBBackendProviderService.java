package org.chronos.chronodb.internal.impl.base.builder.database.service;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.builder.database.spi.ChronoDBBackendProvider;
import org.chronos.chronodb.api.exceptions.ChronoDBConfigurationException;

import java.util.Collections;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ChronoDBBackendProviderService {

    // =================================================================================================================
    // SINGLETON
    // =================================================================================================================


    private static final ChronoDBBackendProviderService INSTANCE = new ChronoDBBackendProviderService();

    public static ChronoDBBackendProviderService getInstance() {
        return INSTANCE;
    }

    private ChronoDBBackendProviderService() {
        // private; use the static instance
    }


    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ServiceLoader<ChronoDBBackendProvider> loader = ServiceLoader.load(ChronoDBBackendProvider.class);

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public ChronoDBBackendProvider getBackendProvider(String chronosBackend) {
        checkNotNull(chronosBackend, "Precondition violation - argument 'chronosBackend' must not be NULL!");
        try {
            Iterator<ChronoDBBackendProvider> iterator = this.loader.iterator();
            Set<ChronoDBBackendProvider> matchingProviders = Sets.newHashSet();
            while (iterator.hasNext()) {
                ChronoDBBackendProvider provider = iterator.next();
                if (provider.matchesBackendName(chronosBackend)) {
                    matchingProviders.add(provider);
                }
            }
            if (matchingProviders.isEmpty()) {
                throw new ChronoDBConfigurationException("No ChronoDB Implementation on the classpath matched the backend '" + chronosBackend + "'! Are you missing a dependency?");
            } else if (matchingProviders.size() > 1) {
                throw new ChronoDBConfigurationException("Multiple ChronoDB Implementations on the classpath matched the backend '" + chronosBackend + "'! Please remove duplicate implementations from your classpath.");
            }
            return Iterables.getOnlyElement(matchingProviders);
        } catch (ServiceConfigurationError e) {
            throw new ChronoDBConfigurationException("An error occurred when trying to detect ChronoDB Implementations on the classpath! See root cause for details.", e);
        }
    }

    public Set<ChronoDBBackendProvider> getAvailableBuilders() {
        Set<ChronoDBBackendProvider> resultSet = Sets.newHashSet();
        Iterables.addAll(resultSet, this.loader);
        return Collections.unmodifiableSet(resultSet);
    }

}