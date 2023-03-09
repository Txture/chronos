package org.chronos.chronograph.internal.impl.transaction.threaded;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.index.IndexType;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.*;

public class ThreadedChronoGraphIndexManager implements ChronoGraphIndexManager, ChronoGraphIndexManagerInternal {

    private final ChronoThreadedTransactionGraph graph;
    private final ChronoGraphIndexManagerInternal wrappedManager;

    public ThreadedChronoGraphIndexManager(final ChronoThreadedTransactionGraph graph, final String branchName) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        this.graph = graph;
        this.wrappedManager = (ChronoGraphIndexManagerInternal) this.graph.getOriginalGraph()
            .getIndexManagerOnBranch(branchName);
    }


    @Override
    public ChronoGraphIndex addIndex(
        final Class<? extends Element> elementType,
        final IndexType indexType,
        final String propertyName,
        final long startTimestamp,
        final long endTimestamp,
        Set<IndexingOption> options
    ) {
        return this.wrappedManager.addIndex(elementType, indexType, propertyName, startTimestamp, endTimestamp, options);
    }

    @Override
    public Iterator<String> findVertexIdsByIndexedProperties(
        final ChronoGraphTransaction tx,
        final Set<SearchSpecification<?, ?>> searchSpecifications
    ) {
        return this.wrappedManager.findVertexIdsByIndexedProperties(tx, searchSpecifications);
    }

    @Override
    public Iterator<String> findEdgeIdsByIndexedProperties(
        final ChronoGraphTransaction tx,
        final Set<SearchSpecification<?, ?>> searchSpecifications
    ) {
        return this.wrappedManager.findEdgeIdsByIndexedProperties(tx, searchSpecifications);
    }

    @Override
    public IndexBuilderStarter create() {
        return this.wrappedManager.create();
    }

    @Override
    public Set<ChronoGraphIndex> getIndexedPropertiesAtTimestamp(final Class<? extends Element> clazz, final long timestamp) {
        return this.wrappedManager.getIndexedPropertiesAtTimestamp(clazz, timestamp);
    }

    @Override
    public Set<ChronoGraphIndex> getIndexedPropertiesAtAnyPointInTime(final Class<? extends Element> clazz) {
        return this.wrappedManager.getIndexedPropertiesAtAnyPointInTime(clazz);
    }


    @Override
    public Set<ChronoGraphIndex> getAllIndicesAtAnyPointInTime() {
        return this.wrappedManager.getAllIndicesAtAnyPointInTime();
    }

    @Override
    public Set<ChronoGraphIndex> getAllIndicesAtTimestamp(final long timestamp) {
        return this.wrappedManager.getAllIndicesAtTimestamp(timestamp);
    }

    @Override
    public ChronoGraphIndex getVertexIndexAtTimestamp(final String indexedPropertyName, final long timestamp) {
        return this.wrappedManager.getVertexIndexAtTimestamp(indexedPropertyName, timestamp);
    }

    @Override
    public Set<ChronoGraphIndex> getVertexIndicesAtAnyPointInTime(final String indexedPropertyName) {
        return this.wrappedManager.getVertexIndicesAtAnyPointInTime(indexedPropertyName);
    }

    @Override
    public ChronoGraphIndex getEdgeIndexAtTimestamp(final String indexedPropertyName, final long timestamp) {
        return this.wrappedManager.getEdgeIndexAtTimestamp(indexedPropertyName, timestamp);
    }

    @Override
    public Set<ChronoGraphIndex> getEdgeIndicesAtAnyPointInTime(final String indexedPropertyName) {
        return this.wrappedManager.getEdgeIndicesAtAnyPointInTime(indexedPropertyName);
    }


    @Override
    public void reindexAll(boolean force) {
        this.wrappedManager.reindexAll(force);
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index) {
        this.wrappedManager.dropIndex(index);
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
    public ChronoGraphIndex terminateIndex(final ChronoGraphIndex index, final long timestamp) {
        return this.wrappedManager.terminateIndex(index, timestamp);
    }

    @Override
    public boolean isReindexingRequired() {
        return this.wrappedManager.isReindexingRequired();
    }

    @Override
    public void withIndexReadLock(final Runnable action) {
        checkNotNull(action, "Precondition violation - argument 'action' must not be NULL!");
        this.wrappedManager.withIndexReadLock(action);
    }

    @Override
    public <T> T withIndexReadLock(final Callable<T> action) {
        checkNotNull(action, "Precondition violation - argument 'action' must not be NULL!");
        return this.wrappedManager.withIndexReadLock(action);
    }

    @Override
    public void withIndexWriteLock(final Runnable action) {
        checkNotNull(action, "Precondition violation - argument 'action' must not be NULL!");
        this.wrappedManager.withIndexWriteLock(action);
    }

    @Override
    public <T> T withIndexWriteLock(final Callable<T> action) {
        checkNotNull(action, "Precondition violation - argument 'action' must not be NULL!");
        return this.wrappedManager.withIndexWriteLock(action);
    }
}
