package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus;
import org.chronos.chronograph.api.structure.PropertyStatus;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.structure.ChronoElementInternal;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleEvent;

import static com.google.common.base.Preconditions.*;

public abstract class ReadOnlyChronoElement implements ChronoElementInternal {

    protected ChronoElement element;

    public ReadOnlyChronoElement(ChronoElement element) {
        checkNotNull(element, "Precondition violation - argument 'element' must not be NULL!");
        this.element = element;
    }

    @Override
    public String id() {
        return this.element.id();
    }

    @Override
    public String label() {
        return this.element.label();
    }

    @Override
    public ChronoGraph graph() {
        return new ReadOnlyChronoGraph(this.element.graph());
    }

    @Override
    public void remove() {
        this.unsupportedOperation();
    }

    @Override
    public void removeProperty(final String key) {
        this.unsupportedOperation();
    }

    @Override
    public boolean isRemoved() {
        return this.element.isRemoved();
    }

    @Override
    public ChronoGraphTransaction getOwningTransaction() {
        return new ReadOnlyChronoGraphTransaction(this.element.getOwningTransaction());
    }

    @Override
    public long getLoadedAtRollbackCount() {
        return this.element.getLoadedAtRollbackCount();
    }

    @Override
    public void updateLifecycleStatus(final ElementLifecycleEvent status) {
        this.unsupportedOperation();
    }


    @Override
    public ElementLifecycleStatus getStatus() {
        return this.element.getStatus();
    }

    @Override
    public PropertyStatus getPropertyStatus(final String propertyKey) {
        return this.element.getPropertyStatus(propertyKey);
    }

    protected <T> T unsupportedOperation() {
        throw new UnsupportedOperationException("Unsupported operation for readOnly graph!");
    }

    @Override
    public void notifyPropertyChanged(final ChronoProperty<?> chronoProperty) {
        throw new UnsupportedOperationException("Properties must not change in a readOnly graph!");
    }
}
