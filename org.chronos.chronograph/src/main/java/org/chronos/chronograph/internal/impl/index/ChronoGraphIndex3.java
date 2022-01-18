package org.chronos.chronograph.internal.impl.index;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.chronodb.api.indexing.LongIndexer;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexInternal;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphIndex3 implements ChronoGraphIndexInternal {

    // =================================================================================================================
    // FACTORY
    // =================================================================================================================

    public static ChronoGraphIndex3 createFromChronoDBIndexOrNull(SecondaryIndex secondaryIndex) {
        if (secondaryIndex == null) {
            return null;
        }
        if (secondaryIndex.getIndexer() instanceof GraphPropertyIndexer<?> == false) {
            return null;
        }
        return new ChronoGraphIndex3(secondaryIndex);
    }

    // =====================================================================================================================
    // FIELDS
    // =====================================================================================================================

    protected SecondaryIndex index;

    // =====================================================================================================================
    // CONSTRUCTOR
    // =====================================================================================================================

    protected ChronoGraphIndex3() {
        // default constructor for serialization
    }

    public ChronoGraphIndex3(final SecondaryIndex index) {
        checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
        checkArgument(index.getIndexer() instanceof GraphPropertyIndexer<?>, "The given secondary index is not a graph index.");
        this.index = index;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public String getId() {
        return this.index.getId();
    }

    @Override
    public String getParentIndexId() {
        return this.index.getParentIndexId();
    }

    @Override
    public String getBackendIndexKey() {
        return this.index.getName();
    }

    @Override
    public Set<IndexingOption> getIndexingOptions() {
        return Collections.unmodifiableSet(this.index.getOptions());
    }

    @Override
    public String getBranch() {
        return this.index.getBranch();
    }

    @Override
    public Period getValidPeriod() {
        return this.index.getValidPeriod();
    }

    @Override
    public String getIndexedProperty() {
        return this.getIndexer().getGraphElementPropertyName();
    }

    @Override
    public IndexType getIndexType() {
        Indexer<?> indexer = this.getIndexer();
        if (indexer instanceof StringIndexer) {
            return IndexType.STRING;
        } else if (indexer instanceof LongIndexer) {
            return IndexType.LONG;
        } else if (indexer instanceof DoubleIndexer) {
            return IndexType.DOUBLE;
        } else {
            throw new IllegalArgumentException("The given indexer has an unknown index type: '" + indexer.getClass().getName() + "'");
        }
    }

    @Override
    public Class<? extends Element> getIndexedElementClass() {
        Indexer<?> indexer = this.getIndexer();
        if (indexer instanceof VertexRecordPropertyIndexer2 || indexer instanceof VertexRecordPropertyIndexer) {
            return Vertex.class;
        } else if (indexer instanceof EdgeRecordPropertyIndexer2 || indexer instanceof EdgeRecordPropertyIndexer) {
            return Edge.class;
        } else {
            throw new IllegalArgumentException("The given indexer has an unknown Element type: '" + indexer.getClass().getName() + "'");
        }
    }

    @Override
    public boolean isDirty() {
        return this.index.getDirty();
    }

    // =================================================================================================================
    // HASH CODE, EQUALS, TOSTRING
    // =================================================================================================================

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ChronoGraphIndex3 that = (ChronoGraphIndex3) o;
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    @Override
    public String toString() {
        return "GraphIndex[" +
            "id='" + this.getId() + "', " +
            "parent='" + this.getParentIndexId() + "', " +
            "elementType='" + this.getIndexedElementClass().getSimpleName() + "', " +
            "valueType='" + this.getIndexType() + "', " +
            "propertyName='" + this.getIndexedProperty() + "', " +
            "branch='" + this.getBranch() + "', " +
            "validPeriod='" + this.getValidPeriod() + "'" +
            "]";
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private GraphPropertyIndexer<?> getIndexer() {
        return (GraphPropertyIndexer<?>)this.index.getIndexer();
    }

}
