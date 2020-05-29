package org.chronos.chronodb.internal.impl.builder.database;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.chronos.common.builder.AbstractChronoBuilder;
import org.chronos.common.builder.ChronoBuilder;

public abstract class AbstractChronoDBBuilder<SELF extends ChronoBuilder<?>> extends AbstractChronoBuilder<SELF>
		implements ChronoBuilder<SELF> {

	protected Configuration getConfiguration() {
		return new MapConfiguration(this.getProperties());
	}

}
