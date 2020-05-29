package org.chronos.chronograph.internal.impl.iterators.builder;

import org.chronos.chronograph.api.iterators.ChronoGraphIteratorBuilder;
import org.chronos.chronograph.api.structure.ChronoGraph;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractChronoGraphIteratorBuilder implements ChronoGraphIteratorBuilder {

    private final BuilderConfig config;

    protected AbstractChronoGraphIteratorBuilder(BuilderConfig config) {
        checkNotNull(config, "Precondition violation - argument 'config' must not be NULL!");
        this.config = config;
    }

    @Override
    public ChronoGraph getGraph() {
        return this.config.getGraph();
    }

    protected BuilderConfig getConfig() {
        return this.config;
    }
}
