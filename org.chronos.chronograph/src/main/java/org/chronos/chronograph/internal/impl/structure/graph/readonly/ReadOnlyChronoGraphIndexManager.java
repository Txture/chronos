package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphIndexManager implements ChronoGraphIndexManager {

    private final ChronoGraphIndexManager manager;

    public ReadOnlyChronoGraphIndexManager(ChronoGraphIndexManager manager){
        checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
        this.manager = manager;
    }

    @Override
    public IndexBuilderStarter create() {
        return this.unsupportedOperation();
    }

    @Override
    public Set<ChronoGraphIndex> getIndexedPropertiesOf(final Class<? extends Element> clazz) {
        Set<ChronoGraphIndex> indexedProperties = this.manager.getIndexedPropertiesOf(clazz);
        return Collections.unmodifiableSet(indexedProperties);
    }

    @Override
    public void reindexAll(boolean force) {
        this.unsupportedOperation();
    }

    @Override
    public void reindex(final ChronoGraphIndex index) {
        this.unsupportedOperation();
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index) {
        this.unsupportedOperation();
    }

    @Override
    public void dropIndex(final ChronoGraphIndex index, Object commitMetadata) {
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
    public boolean isReindexingRequired() {
        return this.manager.isReindexingRequired();
    }

    @Override
    public Set<ChronoGraphIndex> getDirtyIndices() {
        return this.manager.getDirtyIndices();
    }

    private <T> T unsupportedOperation(){
        throw new UnsupportedOperationException("This operation is not supported in a read-only graph!");
    }
}
