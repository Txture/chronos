package org.chronos.chronodb.internal.impl.builder.database;

import static com.google.common.base.Preconditions.*;

import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.builder.database.ChronoDBBaseBuilder;
import org.chronos.chronodb.api.builder.database.spi.ChronoDBBackendProvider;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.ChronoDBFactoryInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.impl.base.builder.database.service.ChronoDBBackendProviderService;

public class ChronoDBFactoryImpl implements ChronoDBFactoryInternal {

	// =================================================================================================================
	// INTERNAL UTILITY METHODS
	// =================================================================================================================

	@Override
	public ChronoDB create(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		String backend = configuration.getString(ChronoDBConfiguration.STORAGE_BACKEND);
		if(backend == null || backend.trim().isEmpty()){
			throw new IllegalArgumentException("The given configuration does not specify a storage backend. Please specify a value for the key '" + ChronoDBConfiguration.STORAGE_BACKEND + "' in your configuration.");
		}
		ChronoDBBackendProvider builderProvider = ChronoDBBackendProviderService.getInstance().getBackendProvider(backend);
		ChronoDBInternal db = builderProvider.instantiateChronoDB(configuration);
		db.postConstruct();
		return db;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public ChronoDBBaseBuilder create() {
		return new ChronoDBBaseBuilderImpl();
	}

}
