package org.chronos.chronograph.internal.impl.iterators.builder;

import org.chronos.chronograph.api.iterators.ChronoGraphBranchIteratorBuilder;
import org.chronos.chronograph.api.iterators.ChronoGraphRootIteratorBuilder;
import org.chronos.chronograph.api.iterators.callbacks.BranchChangeCallback;
import org.chronos.chronograph.api.structure.ChronoGraph;

public class ChronoGraphRootIteratorBuilderImpl extends AbstractChronoGraphIteratorBuilder implements ChronoGraphRootIteratorBuilder {

    public ChronoGraphRootIteratorBuilderImpl(final ChronoGraph graph) {
        super(new BuilderConfig(graph));
    }

    @Override
    public ChronoGraphBranchIteratorBuilder overBranches(final Iterable<String> branchNames, final BranchChangeCallback callback) {
        BuilderConfig config = new BuilderConfig(this.getConfig());
        config.setBranchNames(branchNames);
        config.setBranchChangeCallback(callback);
        return new ChronoGraphBranchIteratorBuilderImpl(config);
    }
}
