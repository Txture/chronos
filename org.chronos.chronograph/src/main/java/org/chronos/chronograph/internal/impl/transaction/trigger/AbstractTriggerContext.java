package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.trigger.AncestorState;
import org.chronos.chronograph.api.transaction.trigger.CurrentState;
import org.chronos.chronograph.api.transaction.trigger.StoreState;
import org.chronos.chronograph.api.transaction.trigger.TriggerContext;
import org.chronos.chronograph.internal.impl.structure.graph.readonly.ReadOnlyChronoGraph;
import org.chronos.chronograph.internal.impl.util.CachedSupplier;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractTriggerContext implements TriggerContext {

    private final GraphBranch graphBranch;
    private final Object commitMetadata;
    private final CurrentState currentState;

    private final CachedSupplier<ChronoGraph> ancestorStateGraphSupplier;
    private final CachedSupplier<ChronoGraph> storeStateGraphSupplier;
    private final CachedSupplier<AncestorState> ancestorStateSupplier;
    private final CachedSupplier<StoreState> storeStateSupplier;

    private String triggerName;
    protected boolean closed = false;

    protected AbstractTriggerContext(final GraphBranch branch, final Object commitMetadata, ChronoGraph currentStateGraph, Supplier<ChronoGraph> ancestorStateGraphSupplier, Supplier<ChronoGraph> storeStateGraphSupplier){
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkNotNull(currentStateGraph, "Precondition violation - argument 'currentStateGraph' must not be NULL!");
        checkNotNull(ancestorStateGraphSupplier, "Precondition violation - argument 'ancestorStateGraphSupplier' must not be NULL!");
        checkNotNull(storeStateGraphSupplier, "Precondition violation - argument 'storeStateGraphSupplier' must not be NULL!");
        this.graphBranch = branch;
        this.commitMetadata = commitMetadata;
        this.currentState = new CurrentStateImpl(this.wrapCurrentStateGraph(currentStateGraph));
        this.ancestorStateGraphSupplier = CachedSupplier.create(() -> this.wrapAncestorStateGraph(ancestorStateGraphSupplier.get()));
        this.storeStateGraphSupplier = CachedSupplier.create(() -> this.wrapStoreStateGraph(storeStateGraphSupplier.get()));
        this.ancestorStateSupplier = this.ancestorStateGraphSupplier.map(AncestorStateImpl::new);
        this.storeStateSupplier = this.storeStateGraphSupplier.map(StoreStateImpl::new);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public String getTriggerName() {
        return triggerName;
    }

    @Override
    public GraphBranch getBranch() {
        return this.graphBranch;
    }

    @Override
    public Object getCommitMetadata() {
        return this.commitMetadata;
    }

    @Override
    public CurrentState getCurrentState() {
        this.assertNotClosed();
        return this.currentState;
    }

    @Override
    public AncestorState getAncestorState() {
        this.assertNotClosed();
        return this.ancestorStateSupplier.get();
    }

    @Override
    public StoreState getStoreState() {
        this.assertNotClosed();
        return this.storeStateSupplier.get();
    }

    @Override
    public void close() {
        if(this.closed){
            return;
        }
        this.ancestorStateGraphSupplier.doIfLoaded(g -> g.tx().rollback());
        this.storeStateGraphSupplier.doIfLoaded(g -> g.tx().rollback());
        this.closed = true;
    }

    protected abstract ChronoGraph wrapCurrentStateGraph(ChronoGraph graph);

    protected ChronoGraph wrapAncestorStateGraph(ChronoGraph graph){
        return new ReadOnlyChronoGraph(graph);
    }

    protected ChronoGraph wrapStoreStateGraph(ChronoGraph graph){
        return new ReadOnlyChronoGraph(graph);
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    public void setTriggerName(String triggerName){
        checkNotNull(triggerName, "Precondition violation - argument 'triggerName' must not be NULL!");
        this.triggerName = triggerName;
    }

    private void assertNotClosed(){
        if(this.closed){
            throw new IllegalStateException("Trigger context has already been closed!");
        }
    }

}
