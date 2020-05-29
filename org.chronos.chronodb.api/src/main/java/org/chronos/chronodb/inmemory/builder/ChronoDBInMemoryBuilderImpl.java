package org.chronos.chronodb.inmemory.builder;

import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.impl.builder.database.AbstractChronoDBFinalizableBuilder;

public class ChronoDBInMemoryBuilderImpl extends AbstractChronoDBFinalizableBuilder<ChronoDBInMemoryBuilder>
		implements ChronoDBInMemoryBuilder {

	public ChronoDBInMemoryBuilderImpl() {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, InMemoryChronoDB.BACKEND_NAME);
	}

}
