package org.chronos.chronosphere.internal.builder.repository.impl;

import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronosphere.api.builder.repository.ChronoSphereInMemoryBuilder;

public class ChronoSphereInMemoryBuilderImpl extends AbstractChronoSphereFinalizableBuilder<ChronoSphereInMemoryBuilder>
		implements ChronoSphereInMemoryBuilder {

	public ChronoSphereInMemoryBuilderImpl() {
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, InMemoryChronoDB.BACKEND_NAME);
	}

}
