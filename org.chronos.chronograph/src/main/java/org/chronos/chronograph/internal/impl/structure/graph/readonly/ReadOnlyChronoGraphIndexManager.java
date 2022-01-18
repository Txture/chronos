package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;

import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphIndexManager implements ChronoGraphIndexManager {

    private final ChronoGraphIndexManager manager;

    public ReadOnlyChronoGraphIndexManager(ChronoGraphIndexManager manager) {
        checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
        this.manager = manager;
    }

    @Override
    public IndexBuilderStarter create() {
        return this.unsupportedOperation();
    }

    @Override
    public Set<ChronoGraphIndex> getIndexedPropertiesAtTimestamp(final Class<? extends Element> clazz, final long timestamp) {
        return this.manager.getIndexedPropertiesAtTimestamp(clazz, timestamp);
    }

    @Override
    public Set<ChronoGraphIndex> getIndexedPropertiesAtAnyPointInTime(final Class<? extends Element> clazz) {
        return this.manager.getIndexedPropertiesAtAnyPointInTime(clazz);
    }

    @Override
    public Set<ChronoGraphIndex> getAllIndicesAtAnyPointInTime() {
        return this.manager.getAllIndicesAtAnyPointInTime();
    }

    @Override
    public Set<ChronoGraphIndex> getAllIndicesAtTimestamp(final long timestamp) {
        return this.manager.getAllIndicesAtTimestamp(timestamp);
    }

    @Override
    public ChronoGraphIndex getVertexIndexAtTimestamp(final String indexedPropertyName, final long timestamp) {
        return this.manager.getVertexIndexAtTimestamp(indexedPropertyName, timestamp);
    }

    @Override
    public Set<ChronoGraphIndex> getVertexIndicesAtAnyPointInTime(final String indexedPropertyName) {
        return this.manager.getVertexIndicesAtAnyPointInTime(indexedPropertyName);
    }

    @Override
    public ChronoGraphIndex getEdgeIndexAtTimestamp(final String indexedPropertyName, final long timestamp) {
        return this.manager.getEdgeIndexAtTimestamp(indexedPropertyName, timestamp);
    }

    @Override
    public Set<ChronoGraphIndex> getEdgeIndicesAtAnyPointInTime(final String indexedPropertyName) {
        return this.manager.getEdgeIndicesAtAnyPointInTime(indexedPropertyName);
    }

    @Override
    public void reindexAll(boolean force) {
        this.unsupportedOperation();
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index) {
        this.unsupportedOperation();
    }

    @Override
    public void dropAllIndices() {
        this.unsupportedOperation();
    }

    @Override
    public void dropAllIndices(Object commitMetadata) {
        this.unsupportedOperation();
    }

    @Override
    public ChronoGraphIndex terminateIndex(final ChronoGraphIndex index, final long timestamp) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean isReindexingRequired() {
        return this.manager.isReindexingRequired();
    }

    private <T> T unsupportedOperation() {
        throw new UnsupportedOperationException("This operation is not supported in a read-only graph!");
    }
}
