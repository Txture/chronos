package org.chronos.chronograph.internal.impl.iterators.builder;

import org.chronos.chronograph.api.iterators.callbacks.BranchChangeCallback;
import org.chronos.chronograph.api.iterators.callbacks.TimestampChangeCallback;
import org.chronos.chronograph.api.structure.ChronoGraph;

import java.util.Iterator;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class BuilderConfig {

    private ChronoGraph graph;
    private Iterable<String> branchNames;
    private BranchChangeCallback branchChangeCallback;
    private Function<String, Iterator<Long>> branchToTimestampsFunction;
    private TimestampChangeCallback timestampChangeCallback;

    public BuilderConfig(ChronoGraph graph) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        this.graph = graph;
    }

    public BuilderConfig(BuilderConfig other) {
        checkNotNull(other, "Precondition violation - argument 'other' must not be NULL!");
        this.graph = other.getGraph();
        this.branchNames = other.getBranchNames();
        this.branchChangeCallback = other.getBranchChangeCallback();
        this.branchToTimestampsFunction = other.getBranchToTimestampsFunction();
        this.timestampChangeCallback = other.getTimestampChangeCallback();
    }

    public ChronoGraph getGraph() {
        return this.graph;
    }

    public void setGraph(final ChronoGraph graph) {
        this.graph = graph;
    }

    public Iterable<String> getBranchNames() {
        return this.branchNames;
    }

    public void setBranchNames(final Iterable<String> branchNames) {
        this.branchNames = branchNames;
    }

    public BranchChangeCallback getBranchChangeCallback() {
        return this.branchChangeCallback;
    }

    public void setBranchChangeCallback(final BranchChangeCallback branchChangeCallback) {
        this.branchChangeCallback = branchChangeCallback;
    }

    public Function<String, Iterator<Long>> getBranchToTimestampsFunction() {
        return this.branchToTimestampsFunction;
    }

    public void setBranchToTimestampsFunction(final Function<String, Iterator<Long>> branchToTimestampsFunction) {
        this.branchToTimestampsFunction = branchToTimestampsFunction;
    }

    public TimestampChangeCallback getTimestampChangeCallback() {
        return this.timestampChangeCallback;
    }

    public void setTimestampChangeCallback(final TimestampChangeCallback timestampChangeCallback) {
        this.timestampChangeCallback = timestampChangeCallback;
    }
}
