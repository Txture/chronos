package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.internal.api.structure.ChronoElementInternal;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class ReadOnlyChronoEdge extends ReadOnlyChronoElement implements ChronoEdge {

    public ReadOnlyChronoEdge(Edge edge) {
        this((ChronoEdge) edge);
    }

    public ReadOnlyChronoEdge(ChronoEdge edge) {
        super(edge);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Iterator<Vertex> vertices(final Direction direction) {
        Iterator<ChronoVertex> iterator = (Iterator) this.edge().vertices(direction);
        return Iterators.transform(iterator, ReadOnlyChronoVertex::new);
    }

    @Override
    public Vertex outVertex() {
        return new ReadOnlyChronoVertex((ChronoVertex) this.edge().outVertex());
    }

    @Override
    public Vertex inVertex() {
        return new ReadOnlyChronoVertex((ChronoVertex) this.edge().inVertex());
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Iterator<Vertex> bothVertices() {
        Iterator<ChronoVertex> iterator = (Iterator) this.edge().bothVertices();
        return Iterators.transform(iterator, ReadOnlyChronoVertex::new);
    }

    @Override
    public Set<String> keys() {
        return Collections.unmodifiableSet(this.edge().keys());
    }

    @Override
    public <V> Property<V> property(final String key) {
        return new ReadOnlyProperty<>(this.edge().property(key));
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return this.unsupportedOperation();
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return this.edge().value(key);
    }

    @Override
    public <V> Iterator<V> values(final String... propertyKeys) {
        Iterator<V> iterator = this.edge().values(propertyKeys);
        return Iterators.unmodifiableIterator(iterator);
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        Iterator<Property<V>> properties = this.edge().properties(propertyKeys);
        return Iterators.transform(properties, ReadOnlyProperty::new);
    }


    @Override
    public void validateGraphInvariant() {
        ((ChronoElementInternal)this.edge()).validateGraphInvariant();
    }

    protected ChronoEdge edge() {
        return (ChronoEdge) this.element;
    }
}
