package org.chronos.chronograph.internal.impl.structure.graph;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.structure.ChronoElementInternal;
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal;
import org.chronos.chronograph.internal.impl.structure.record2.PropertyRecord2;

import java.util.NoSuchElementException;

public class ChronoProperty<V> implements Property<V> {

    private final ChronoElementInternal owner;
    private final String key;
    private V value;

    private boolean removed;

    public ChronoProperty(final ChronoElementInternal owner, final String key, final V value) {
        this(owner, key, value, false);
    }

    public ChronoProperty(final ChronoElementInternal owner, final String key, final V value, final boolean silent) {
        this.owner = owner;
        this.key = key;
        this.set(value, silent);
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        if (this.removed) {
            throw new NoSuchElementException("Property '" + this.key + "' was removed and is no longer present!");
        }
        return this.value;
    }

    public void set(final V value) {
        this.set(value, false);
    }

    private void set(final V value, final boolean silent) {
        if (this.removed) {
            throw new NoSuchElementException("Property '" + this.key + "' was removed and is no longer present!");
        }
        if (value == null) {
            this.remove();
        } else {
            this.value = value;
            if (silent == false) {
                this.element().notifyPropertyChanged(this);
                this.getTransactionContext().markPropertyAsModified(this);
            }
        }
    }

    @Override
    public boolean isPresent() {
        return this.removed == false;
    }

    @Override
    public ChronoElementInternal element() {
        return this.owner;
    }

    @Override
    public void remove() {
        if (this.removed) {
            return;
        }
        this.getTransactionContext().markPropertyAsDeleted(this);
        this.removed = true;
        this.owner.removeProperty(this.key);
    }

    public boolean isRemoved() {
        return this.removed;
    }

    public IPropertyRecord toRecord() {
        return new PropertyRecord2(this.key, this.value);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(final Object obj) {
        return ElementHelper.areEqual(this, obj);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    protected ChronoGraphTransaction getGraphTransaction() {
        return this.owner.graph().tx().getCurrentTransaction();
    }

    protected GraphTransactionContextInternal getTransactionContext() {
        return (GraphTransactionContextInternal) this.getGraphTransaction().getContext();
    }

}
