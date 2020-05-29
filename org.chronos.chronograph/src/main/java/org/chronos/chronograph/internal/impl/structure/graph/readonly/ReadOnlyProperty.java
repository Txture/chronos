package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyProperty<V> implements Property<V> {

    protected final Property<V> property;

    public ReadOnlyProperty(Property<V> property) {
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        this.property = property;
    }

    @Override
    public String key() {
        return this.property.key();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.property.value();
    }

    @Override
    public boolean isPresent() {
        return this.property.isPresent();
    }

    @Override
    public Element element() {
        Element element = this.property.element();
        if (element instanceof Vertex) {
            return new ReadOnlyChronoVertex((Vertex) element);
        } else if (element instanceof Edge) {
            return new ReadOnlyChronoEdge((Edge) element);
        } else {
            throw new RuntimeException("Encountered unknown subclass of " + Element.class.getName() + ": " + this.element().getClass().getName());
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This operation is not supported in a read-only graph!");
    }
}
