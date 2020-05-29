package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.*;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ReadOnlyVertexProperty<V> extends ReadOnlyProperty<V> implements VertexProperty<V> {

    public ReadOnlyVertexProperty(final VertexProperty<V> property) {
        super(property);
    }

    @Override
    public Object id() {
        return this.vertexProperty().id();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw new UnsupportedOperationException("This operation is not supported in a read-only graph!");
    }

    @Override
    public <V> V value(final String key) throws NoSuchElementException {
        return null;
    }

    @Override
    public <V> Iterator<V> values(final String... propertyKeys) {
        return null;
    }

    @Override
    public Graph graph() {
        return new ReadOnlyChronoGraph((ChronoGraph) this.vertexProperty().graph());
    }

    @Override
    public Vertex element() {
        return new ReadOnlyChronoVertex((ChronoVertex)this.vertexProperty().element());
    }

    @Override
    public Set<String> keys() {
        return Collections.unmodifiableSet(this.vertexProperty().keys());
    }

    @Override
    public <V> Property<V> property(final String key) {
        return new ReadOnlyProperty<>(this.vertexProperty().property(key));
    }

    @Override
    public String label() {
        return this.vertexProperty().label();
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        Iterator<Property<U>> properties = this.vertexProperty().properties(propertyKeys);
        return Iterators.transform(properties, ReadOnlyProperty::new);
    }


    private VertexProperty<V> vertexProperty(){
        return (VertexProperty<V>)this.property;
    }

    @Override
    public void ifPresent(final Consumer<? super V> consumer) {
        this.vertexProperty().ifPresent(consumer);
    }

    @Override
    public V orElse(final V otherValue) {
        return this.vertexProperty().orElse(otherValue);
    }

    @Override
    public V orElseGet(final Supplier<? extends V> valueSupplier) {
        return this.vertexProperty().orElseGet(valueSupplier);
    }

    @Override
    public <E extends Throwable> V orElseThrow(final Supplier<? extends E> exceptionSupplier) throws E {
        return this.vertexProperty().orElseThrow(exceptionSupplier);
    }
}
