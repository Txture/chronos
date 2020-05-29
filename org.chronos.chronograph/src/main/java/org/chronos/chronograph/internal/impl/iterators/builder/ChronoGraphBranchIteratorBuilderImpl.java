package org.chronos.chronograph.internal.impl.iterators.builder;

import org.chronos.chronograph.api.iterators.ChronoGraphBranchIteratorBuilder;
import org.chronos.chronograph.api.iterators.ChronoGraphTimestampIteratorBuilder;
import org.chronos.chronograph.api.iterators.callbacks.TimestampChangeCallback;

import java.util.Iterator;
import java.util.function.Function;

public class ChronoGraphBranchIteratorBuilderImpl extends AbstractChronoGraphIteratorBuilder implements ChronoGraphBranchIteratorBuilder {

    public ChronoGraphBranchIteratorBuilderImpl(final BuilderConfig config) {
        super(config);
    }

    @Override
    public ChronoGraphTimestampIteratorBuilder overTimestamps(final Function<String, Iterator<Long>> branchToTimestampsFunction, final TimestampChangeCallback callback) {
        BuilderConfig config = new BuilderConfig(this.getConfig());
        config.setBranchToTimestampsFunction(branchToTimestampsFunction);
        config.setTimestampChangeCallback(callback);
        return new ChronoGraphTimestampIteratorBuilderImpl(config);
    }
}
