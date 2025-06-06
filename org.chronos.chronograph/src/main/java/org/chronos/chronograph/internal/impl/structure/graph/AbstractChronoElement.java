package org.chronos.chronograph.internal.impl.structure.graph;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus.IllegalStateTransitionException;
import org.chronos.chronograph.api.structure.PropertyStatus;
import org.chronos.chronograph.internal.api.structure.ChronoElementInternal;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal;
import org.chronos.chronograph.internal.impl.transaction.ElementLoadMode;
import org.chronos.chronograph.internal.impl.transaction.threaded.ChronoThreadedTransactionGraph;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractChronoElement implements ChronoElementInternal {

    protected final String id;
    protected String label;
    protected final ChronoGraphInternal graph;

    protected final Thread owningThread;
    protected long loadedAtRollbackCount;
    protected ChronoGraphTransactionInternal owningTransaction;


    private int skipRemovedCheck;
    private int skipModificationCheck;

    private ElementLifecycleStatus status;

    private Map<String, PropertyStatus> propertyStatusMap;

    protected AbstractChronoElement(final ChronoGraphInternal g, final ChronoGraphTransactionInternal tx,
                                    final String id, final String label) {
        checkNotNull(g, "Precondition violation - argument 'g' must not be NULL!");
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
        this.graph = g;
        this.owningTransaction = tx;
        this.loadedAtRollbackCount = tx.getRollbackCount();
        this.id = id;
        this.status = ElementLifecycleStatus.NEW;
        this.label = label;
        this.owningThread = Thread.currentThread();
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public ChronoGraphInternal graph() {
        return this.graph;
    }

    @Override
    public void remove() {
        this.checkAccess();
        // removing an element can start a transaction as well
        this.graph().tx().readWrite();
        this.updateLifecycleStatus(ElementLifecycleEvent.DELETED);
    }

    @Override
    public boolean isRemoved() {
        return this.status.equals(ElementLifecycleStatus.REMOVED)
            || this.status.equals(ElementLifecycleStatus.OBSOLETE);
    }


    @Override
    public void updateLifecycleStatus(final ElementLifecycleEvent event) {
        checkNotNull(event, "Precondition violation - argument 'event' must not be NULL!");
        if (this.isModificationCheckActive()) {
            try {
                this.status = this.status.nextState(event);
            } catch (IllegalStateTransitionException e) {
                this.throwIllegalStateSwitchException(event, e);
            }
        }
    }

    @Override
    public ElementLifecycleStatus getStatus() {
        return this.status;
    }

    // =================================================================================================================
    // HASH CODE & EQUALS
    // =================================================================================================================

    @Override
    public final int hashCode() {
        // according to TinkerGraph reference implementation
        return ElementHelper.hashCode(this);
    }

    @Override
    public final boolean equals(final Object object) {
        // according to TinkerGraph reference implementation
        return ElementHelper.areEqual(this, object);
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    @Override
    public ChronoGraphTransactionInternal getOwningTransaction() {
        return this.owningTransaction;
    }

    public Thread getOwningThread() {
        return this.owningThread;
    }

    protected ChronoVertex resolveVertex(final String id) {
        return (ChronoVertex) this.getOwningTransaction().getVertex(id, ElementLoadMode.LAZY);
    }

    protected ChronoEdge resolveEdge(final String id) {
        Iterator<Edge> iterator = this.graph.edges(id);
        return (ChronoEdge) Iterators.getOnlyElement(iterator);
    }

    protected ChronoGraphTransactionInternal getGraphTransaction() {
        return (ChronoGraphTransactionInternal) this.graph().tx().getCurrentTransaction();
    }

    protected GraphTransactionContextInternal getTransactionContext() {
        return (GraphTransactionContextInternal) this.getGraphTransaction().getContext();
    }

    @Override
    public long getLoadedAtRollbackCount() {
        return this.loadedAtRollbackCount;
    }

    // =====================================================================================================================
    // INTERNAL UTILITY METHODS
    // =====================================================================================================================

    protected boolean isModificationCheckActive() {
        return this.skipModificationCheck <= 0;
    }

    protected void assertNotRemoved() {
        if (this.isRemoved() && this.skipRemovedCheck <= 0) {
            String elementType;
            if (this instanceof Vertex) {
                elementType = "Vertex";
            } else if (this instanceof Edge) {
                elementType = "Edge";
            } else {
                elementType = "Element";
            }
            throw new IllegalStateException("The " + elementType + " '" + this.id + "' has already been removed!");
        }
    }

    protected void withoutRemovedCheck(final Runnable r) {
        this.skipRemovedCheck++;
        try {
            r.run();
        } finally {
            this.skipRemovedCheck--;
        }
    }

    protected <T> T withoutRemovedCheck(final Callable<T> c) {
        this.skipRemovedCheck++;
        try {
            try {
                return c.call();
            } catch (Exception e) {
                throw new RuntimeException("Error during method execution", e);
            }
        } finally {
            this.skipRemovedCheck--;
        }
    }

    protected void withoutModificationCheck(final Runnable r) {
        this.skipModificationCheck++;
        try {
            r.run();
        } finally {
            this.skipModificationCheck--;
        }
    }

    protected <T> T withoutModificationCheck(final Callable<T> c) {
        this.skipModificationCheck++;
        try {
            try {
                return c.call();
            } catch (Exception e) {
                throw new RuntimeException("Error during method execution", e);
            }
        } finally {
            this.skipModificationCheck--;
        }
    }

    public void checkAccess() {
        this.checkThread();
        this.checkTransaction();
        this.assertNotRemoved();
    }

    public void checkThread() {
        if (this.owningTransaction.isThreadedTx()) {
            // if we are owned by a threaded transaction, any thread has access to the element.
            return;
        }
        Thread currentThread = Thread.currentThread();
        if (currentThread.equals(this.getOwningThread()) == false) {
            throw new IllegalStateException("Graph Elements generated by a thread-bound transaction"
                + " are not safe for concurrent access! Do not share them among threads! Owning thread is '"
                + this.getOwningThread().getName() + "', current thread is '" + currentThread.getName() + "'.");
        }
    }

    public void checkTransaction() {
        if (this.owningTransaction.isThreadedTx()) {
            // threaded tx
            this.owningTransaction.assertIsOpen();
        } else {
            // thread-local tx
            this.graph.tx().readWrite();
            ChronoGraphTransactionInternal currentTx = (ChronoGraphTransactionInternal) this.graph.tx()
                .getCurrentTransaction();
            if (currentTx.equals(this.getOwningTransaction())) {
                // we are still on the same transaction that created this element
                // Check if a rollback has occurred
                if (this.loadedAtRollbackCount == currentTx.getRollbackCount()) {
                    // same transaction, same rollback count -> nothing to do.
                    return;
                } else {
                    // a rollback has occurred; if the element was modified, we need to reload it
                    if (this.status.isDirty()) {
                        this.reloadFromDatabase();
                        this.updateLifecycleStatus(ElementLifecycleEvent.SAVED);
                    }
                    this.loadedAtRollbackCount = currentTx.getRollbackCount();
                }
            } else {
                // we are on a different transaction, rebind to the new transaction and reload
                this.owningTransaction = currentTx;
                this.loadedAtRollbackCount = this.owningTransaction.getRollbackCount();
                this.reloadFromDatabase();
                this.updateLifecycleStatus(ElementLifecycleEvent.SAVED);
            }
        }

    }

    /**
     * Clears the entire property status cache for all properties of this element.
     */
    protected void clearPropertyStatusCache() {
        this.propertyStatusMap = null;
    }


    /**
     * Changes the status of the {@link Property} with the given key to the given status.
     *
     * @param propertyKey The key of the property to change the status for. Must not be <code>null</code>.
     * @param status      The new status of the property. Must not be <code>null</code>.
     */
    protected void changePropertyStatus(final String propertyKey, final PropertyStatus status) {
        checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
        checkNotNull(status, "Precondition violation - argument 'status' must not be NULL!");
        PropertyStatus currentStatus = this.getPropertyStatus(propertyKey);
        switch (currentStatus) {
            case PERSISTED:
                switch (status) {
                    case MODIFIED:
                        this.assignPropertyStatus(propertyKey, status);
                        break;
                    case NEW:
                        throw new IllegalArgumentException("Cannot switch property state from PERSISTED to NEW!");
                    case PERSISTED:
                        // no-op
                        break;
                    case REMOVED:
                        this.assignPropertyStatus(propertyKey, status);
                        break;
                    case UNKNOWN:
                        throw new IllegalArgumentException("Cannot switch property state from PERSISTED to UNKNOWN!");
                    default:
                        throw new UnknownEnumLiteralException(status);
                }
                break;
            case UNKNOWN:
                switch (status) {
                    case MODIFIED:
                        this.assignPropertyStatus(propertyKey, status);
                        break;
                    case NEW:
                        this.assignPropertyStatus(propertyKey, status);
                        break;
                    case PERSISTED:
                        throw new IllegalArgumentException("Cannot switch property state from UNKNOWN to PERSISTED!");
                    case REMOVED:
                        this.assignPropertyStatus(propertyKey, status);
                        break;
                    case UNKNOWN:
                        // no-op
                        break;
                    default:
                        throw new UnknownEnumLiteralException(status);
                }
                break;
            case NEW:
                switch (status) {
                    case MODIFIED:
                        // new remains in new, even when modified again -> no-op
                        break;
                    case NEW:
                        // no-op
                        break;
                    case PERSISTED:
                        if (this.propertyStatusMap != null) {
                            // we mark it as persisted by simply dropping the metadata, the getter will
                            // recreate it on-demand and we save the map entry
                            this.propertyStatusMap.remove(propertyKey);
                        }
                        break;
                    case REMOVED:
                        if (this.propertyStatusMap != null) {
                            // switching from NEW to REMOVED makes the property obsolete, the getter
                            // will recreate it on-demand and we save the map entry
                            this.propertyStatusMap.remove(propertyKey);
                        }
                        break;
                    case UNKNOWN:
                        throw new IllegalArgumentException("Cannot switch property state from NEW to UNKNOWN!");
                    default:
                        throw new UnknownEnumLiteralException(status);
                }
                break;
            case MODIFIED:
                switch (status) {
                    case MODIFIED:
                        // no-op
                        break;
                    case NEW:
                        throw new IllegalArgumentException("Cannot switch property state from MODIFIED to NEW!");
                    case PERSISTED:
                        if (this.propertyStatusMap != null) {
                            // we mark it as persisted by simply dropping the metadata, the getter will
                            // recreate it on-demand and we save the map entry
                            this.propertyStatusMap.remove(propertyKey);
                        }
                        break;
                    case REMOVED:
                        this.assignPropertyStatus(propertyKey, status);
                        break;
                    case UNKNOWN:
                        throw new IllegalArgumentException("Cannot switch property state from MODIFIED to UNKNOWN!");
                    default:
                        throw new UnknownEnumLiteralException(status);
                }
                break;
            case REMOVED:
                switch (status) {
                    case MODIFIED:
                        this.assignPropertyStatus(propertyKey, status);
                        break;
                    case NEW:
                        // removal and re-addition = modification
                        this.assignPropertyStatus(propertyKey, PropertyStatus.MODIFIED);
                        break;
                    case PERSISTED:
                        throw new IllegalArgumentException("Cannot switch property state from REMOVED to PERSISTED!");
                    case REMOVED:
                        // no-op
                        break;
                    case UNKNOWN:
                        if (this.propertyStatusMap != null) {
                            // the property was removed, the vertex was saved and the property is now unknown.
                            this.propertyStatusMap.remove(propertyKey);
                        }
                        break;
                    default:
                        throw new UnknownEnumLiteralException(status);
                }
                break;
            default:
                throw new UnknownEnumLiteralException(currentStatus);
        }
    }

    private void assignPropertyStatus(final String propertyKey, final PropertyStatus status) {
        checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
        checkNotNull(status, "Precondition violation - argument 'status' must not be NULL!");
        if (this.propertyStatusMap == null) {
            this.propertyStatusMap = Maps.newHashMap();
        }
        this.propertyStatusMap.put(propertyKey, status);
    }

    /**
     * Returns the current status of the {@link Property} with the given key.
     *
     * <p>
     * The result may be one of the following:
     * <ul>
     * <li>{@link PropertyStatus#UNKNOWN}: indicates that a property with the given key has never existed on this vertex.
     * <li>{@link PropertyStatus#NEW}: the property has been newly added in this transaction.
     * <li>{@link PropertyStatus#MODIFIED}: the property has existed before, but was modified in this transaction.
     * <li>{@link PropertyStatus#REMOVED}: the property has existed before, but was removed in this transaction.
     * <li>{@link PropertyStatus#PERSISTED}: the property exists and is unchanged.
     * </ul>
     *
     * @param propertyKey The property key to search for. Must not be <code>null</code>.
     * @return The status of the property with the given key, as outlined above. Never <code>null</code>.
     */
    @Override
    public PropertyStatus getPropertyStatus(final String propertyKey) {
        checkNotNull(propertyKey, "Precondition violation - argument 'propertyKey' must not be NULL!");
        PropertyStatus status = null;
        if (this.propertyStatusMap != null) {
            status = this.propertyStatusMap.get(propertyKey);
        }
        if (this.propertyStatusMap == null || status == null) {
            if (this.property(propertyKey).isPresent()) {
                // the property with the given key exists, but we have no metadata about it -> it's clean
                return PropertyStatus.PERSISTED;
            } else {
                // the property with the given key doesn't exist -> we've never seen this property before
                return PropertyStatus.UNKNOWN;
            }
        } else {
            return status;
        }
    }

    private void throwIllegalStateSwitchException(final ElementLifecycleEvent event, Exception cause) {
        throw new IllegalStateException("Element Lifecycle error: element '" + this.getClass().getSimpleName() + "' with id '" + this.id()
            + "' in status " + this.getStatus() + " cannot accept the change '" + event + "'!", cause);
    }

    protected abstract void reloadFromDatabase();


}
