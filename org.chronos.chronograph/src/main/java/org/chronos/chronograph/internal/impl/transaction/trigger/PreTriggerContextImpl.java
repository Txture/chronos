package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.trigger.PreCommitTriggerContext;
import org.chronos.chronograph.api.transaction.trigger.PrePersistTriggerContext;
import org.chronos.chronograph.internal.impl.structure.graph.readonly.NoTransactionControlChronoGraph;

import java.util.function.Supplier;

public class PreTriggerContextImpl extends AbstractTriggerContext implements PreCommitTriggerContext, PrePersistTriggerContext {

    public PreTriggerContextImpl(final GraphBranch branch, final Object commitMetadata, ChronoGraph currentStateGraph, Supplier<ChronoGraph> ancestorStateGraphSupplier, Supplier<ChronoGraph> storeStateGraphSupplier) {
        super(branch, commitMetadata, currentStateGraph, ancestorStateGraphSupplier, storeStateGraphSupplier);
    }

    @Override
    protected ChronoGraph wrapCurrentStateGraph(final ChronoGraph graph) {
        return new NoTransactionControlChronoGraph(graph);
    }

}
