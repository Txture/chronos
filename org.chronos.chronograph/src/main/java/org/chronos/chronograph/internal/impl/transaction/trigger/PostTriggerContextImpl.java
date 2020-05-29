package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.trigger.*;
import org.chronos.chronograph.internal.impl.structure.graph.readonly.ReadOnlyChronoGraph;
import org.chronos.chronograph.internal.impl.util.CachedSupplier;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.*;

public class PostTriggerContextImpl extends AbstractTriggerContext implements PostCommitTriggerContext, PostPersistTriggerContext {

    private final long timestamp;

    private final CachedSupplier<ChronoGraph> preCommitStoreStateGraphSupplier;
    private final CachedSupplier<PreCommitStoreState> preCommitStoreStateSupplier;

    public PostTriggerContextImpl(final GraphBranch branch, long timestamp, final Object commitMetadata, ChronoGraph currentStateGraph, Supplier<ChronoGraph> ancestorStateGraphSupplier, Supplier<ChronoGraph> storeStateGraphSupplier, Supplier<ChronoGraph> preCommitStoreStateGraphSupplier) {
        super(branch, commitMetadata, currentStateGraph, ancestorStateGraphSupplier, storeStateGraphSupplier);
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        this.timestamp = timestamp;
        this.preCommitStoreStateGraphSupplier = CachedSupplier.create(() -> this.wrapStoreStateGraph(preCommitStoreStateGraphSupplier.get()));
        this.preCommitStoreStateSupplier = this.preCommitStoreStateGraphSupplier.map(PreCommitStoreStateImpl::new);
    }

    @Override
    public long getCommitTimestamp() {
        return this.timestamp;
    }

    @Override
    public PreCommitStoreState getPreCommitStoreState() {
        return this.preCommitStoreStateSupplier.get();
    }

    @Override
    protected ChronoGraph wrapCurrentStateGraph(final ChronoGraph graph) {
        return new ReadOnlyChronoGraph(graph);
    }

    @Override
    public void close() {
        if(this.closed){
            return;
        }
        super.close();
        this.preCommitStoreStateGraphSupplier.doIfLoaded(g -> g.tx().rollback());
    }
}
