package org.chronos.chronograph.internal.impl.transaction.threaded;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;

import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ThreadedChronoGraphIndexManager implements ChronoGraphIndexManager, ChronoGraphIndexManagerInternal {

    private final ChronoThreadedTransactionGraph graph;
    private final ChronoGraphIndexManagerInternal wrappedManager;

    public ThreadedChronoGraphIndexManager(final ChronoThreadedTransactionGraph graph, final String branchName) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        this.graph = graph;
        this.wrappedManager = (ChronoGraphIndexManagerInternal) this.graph.getOriginalGraph()
            .getIndexManager(branchName);
    }

    @Override
    public void addIndex(final ChronoGraphIndex index) {
        this.wrappedManager.addIndex(index);
    }

    @Override
    public void addIndex(final ChronoGraphIndex index, Object commitMetadata) {
        this.wrappedManager.addIndex(index, commitMetadata);
    }

    @Override
    public Iterator<String> findVertexIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?,?>> searchSpecifications) {
        return this.wrappedManager.findVertexIdsByIndexedProperties(tx, searchSpecifications);
    }

    @Override
    public Iterator<String> findEdgeIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?,?>> searchSpecifications) {
        return this.wrappedManager.findEdgeIdsByIndexedProperties(tx, searchSpecifications);
    }

    @Override
    public IndexBuilderStarter create() {
        return this.wrappedManager.create();
    }

    @Override
    public Set<ChronoGraphIndex> getIndexedPropertiesOf(final Class<? extends Element> clazz) {
        return this.wrappedManager.getIndexedPropertiesOf(clazz);
    }

    @Override
    public void reindexAll(boolean force) {
        this.wrappedManager.reindexAll(force);
    }

    @Override
    public void reindex(final ChronoGraphIndex index) {
        this.wrappedManager.reindexAll();
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index) {
        this.wrappedManager.dropIndex(index);
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index, final Object commitMetadata) {
        this.wrappedManager.dropIndex(index, commitMetadata);
    }

    @Override
    public void dropAllIndices() {
        this.wrappedManager.dropAllIndices();
    }

    @Override
    public void dropAllIndices(Object commitMetadata) {
        this.wrappedManager.dropAllIndices(commitMetadata);
    }

    @Override
    public boolean isReindexingRequired() {
        return this.wrappedManager.isReindexingRequired();
    }

    @Override
    public Set<ChronoGraphIndex> getDirtyIndices() {
        return this.wrappedManager.getDirtyIndices();
    }

}
