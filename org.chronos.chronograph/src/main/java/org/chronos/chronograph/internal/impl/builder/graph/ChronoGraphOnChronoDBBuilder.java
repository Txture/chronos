package org.chronos.chronograph.internal.impl.builder.graph;

import org.chronos.chronodb.api.builder.database.ChronoDBFinalizableBuilder;

import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphOnChronoDBBuilder extends AbstractChronoGraphFinalizableBuilder {

	public ChronoGraphOnChronoDBBuilder(ChronoDBFinalizableBuilder<?> chronoDBBuilder) {
        checkNotNull(chronoDBBuilder, "Precondition violation - argument 'chronoDBBuilder' must not be NULL!");
        Map<String, String> properties = chronoDBBuilder.getProperties();
        for(Entry<String, String> entry : properties.entrySet()){
            this.withProperty(entry.getKey(), entry.getValue());
        }
        ChronoGraphDefaultProperties.applyTo(this);
	}

}
