package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.internal.api.structure.ChronoElementInternal;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ReadOnlyChronoVertex extends ReadOnlyChronoElement implements ChronoVertex {

    public ReadOnlyChronoVertex(Vertex vertex) {
        this((ChronoVertex) vertex);
    }

    public ReadOnlyChronoVertex(final ChronoVertex vertex) {
        super(vertex);
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        return this.unsupportedOperation();
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        return new ReadOnlyVertexProperty<>(this.vertex().property(key));
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.unsupportedOperation();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V value(final String key) throws NoSuchElementException {
        return (V) this.vertex().value(key);
    }

    @Override
    public <V> Iterator<V> values(final String... propertyKeys) {
        Iterator<V> values = this.vertex().values(propertyKeys);
        return Iterators.unmodifiableIterator(values);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        return this.unsupportedOperation();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        return this.unsupportedOperation();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        Iterator<VertexProperty<V>> iterator = this.vertex().properties(propertyKeys);
        return Iterators.transform(iterator, ReadOnlyVertexProperty::new);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        Iterator<ChronoEdge> edges = (Iterator) this.vertex().edges(direction, edgeLabels);
        return Iterators.transform(edges, ReadOnlyChronoEdge::new);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        Iterator<ChronoVertex> vertices = (Iterator) this.vertex().vertices(direction, edgeLabels);
        return Iterators.transform(vertices, ReadOnlyChronoVertex::new);
    }

    @Override
    public void validateGraphInvariant() {
        ((ChronoElementInternal)this.vertex()).validateGraphInvariant();
    }

    protected ChronoVertex vertex() {
        return (ChronoVertex) super.element;
    }
}
