package org.chronos.chronograph.internal.impl.structure.graph;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus;
import org.chronos.chronograph.api.structure.PropertyStatus;
import org.chronos.chronograph.api.structure.record.IVertexPropertyRecord;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.structure.ChronoElementInternal;
import org.chronos.chronograph.internal.impl.structure.record3.SimpleVertexPropertyRecord;
import org.chronos.chronograph.internal.impl.structure.record3.VertexPropertyRecord3;
import org.chronos.chronograph.internal.impl.util.ChronoGraphElementUtil;
import org.chronos.chronograph.internal.impl.util.PredefinedProperty;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ChronoVertexProperty<V> extends ChronoProperty<V> implements VertexProperty<V>, ChronoElementInternal {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final Map<String, ChronoProperty<?>> properties;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ChronoVertexProperty(final ChronoVertexImpl owner, final String key, final V value) {
        super(owner, key, value, true);
        this.properties = Maps.newHashMap();
    }

    // =================================================================================================================
    // TINKERPOP 3 API
    // =================================================================================================================

    @Override
    public String id() {
        return this.element().id() + "->" + this.key();
    }

    @Override
    public String label() {
        return this.key();
    }

    @Override
    public ChronoGraph graph() {
        return this.element().graph();
    }

    @Override
    public ChronoVertexImpl element() {
        return (ChronoVertexImpl) super.element();
    }

    @Override
    public <T> ChronoProperty<T> property(final String key, final T value) {
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        return this.property(key, value, false);
    }

    public <T> ChronoProperty<T> property(final String key, final T value, boolean silent) {
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        ChronoProperty<T> newProperty = new ChronoProperty<>(this, key, value, silent);
        this.properties.put(key, newProperty);
        this.updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
        return newProperty;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <T> Iterator<Property<T>> properties(final String... propertyKeys) {
        Set<Property> matchingProperties = Sets.newHashSet();
        if (propertyKeys == null || propertyKeys.length <= 0) {
            matchingProperties.addAll(this.properties.values());
        } else {
            for (String key : propertyKeys) {
                PredefinedProperty<?> predefinedProperty = ChronoGraphElementUtil.asPredefinedProperty(this, key);
                if (predefinedProperty != null) {
                    matchingProperties.add(predefinedProperty);
                }
                Property property = this.properties.get(key);
                if (property != null) {
                    matchingProperties.add(property);
                }
            }
        }
        return new PropertiesIterator<>(matchingProperties.iterator());
    }

    @Override
    public long getLoadedAtRollbackCount() {
        throw new UnsupportedOperationException("VertexProperties do not support 'loaded at rollback count'!");
    }

    @Override
    public ChronoGraphTransaction getOwningTransaction() {
        throw new UnsupportedOperationException("VertexProperties do not support 'getOwningTransaction'!");
    }

    @Override
    public PropertyStatus getPropertyStatus(final String propertyKey) {
        throw new UnsupportedOperationException("getPropertyStatus(...) is not supported for meta-properties.");
    }

    @Override
    public boolean isLazy() {
        // at the point where we have access to a vertex property,
        // the properties are always loaded.
        return false;
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    @Override
    public void removeProperty(final String key) {
        this.properties.remove(key);
        this.element().notifyPropertyChanged(this);
        this.updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
    }

    @Override
    public void notifyPropertyChanged(final ChronoProperty<?> chronoProperty) {
        // propagate notification to our parent
        this.element().notifyPropertyChanged(this);
    }

    @Override
    public void validateGraphInvariant() {
        // nothing to do here
    }

    @Override
    public IVertexPropertyRecord toRecord() {
        if(this.properties == null || this.properties.isEmpty()){
            // no meta-properties present, use a simple property instead
            return new SimpleVertexPropertyRecord(this.key(), this.value());
        }else{
            // use full-blown property
            return new VertexPropertyRecord3(this.key(), this.value(), this.properties());
        }
    }

    @Override
    public void updateLifecycleStatus(final ElementLifecycleEvent event) {
        if (event != ElementLifecycleEvent.SAVED) {
            this.element().updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
        }
    }

    @Override
    public ElementLifecycleStatus getStatus() {
        if (this.isRemoved()) {
            return ElementLifecycleStatus.REMOVED;
        } else {
            // TODO properties do not separately track their status, using the element status is not entirely correct.
            return this.element().getStatus();
        }
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public boolean equals(final Object obj) {
        return ElementHelper.areEqual(this, obj);
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private class PropertiesIterator<T> implements Iterator<Property<T>> {

        @SuppressWarnings("rawtypes")
        private Iterator<Property> iter;

        @SuppressWarnings("rawtypes")
        public PropertiesIterator(final Iterator<Property> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Property<T> next() {
            Property<T> p = this.iter.next();
            return p;
        }

    }

}
