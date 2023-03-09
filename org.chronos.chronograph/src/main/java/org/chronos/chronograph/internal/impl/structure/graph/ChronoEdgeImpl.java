package org.chronos.chronograph.internal.impl.structure.graph;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronograph.api.exceptions.GraphInvariantViolationException;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.structure.PropertyStatus;
import org.chronos.chronograph.api.structure.record.IEdgeRecord;
import org.chronos.chronograph.api.structure.record.IEdgeTargetRecord;
import org.chronos.chronograph.api.structure.record.IPropertyRecord;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.api.transaction.ChronoGraphTransactionInternal;
import org.chronos.chronograph.internal.impl.structure.record.EdgeRecord;
import org.chronos.chronograph.internal.impl.structure.record2.EdgeRecord2;
import org.chronos.chronograph.internal.impl.util.ChronoGraphElementUtil;
import org.chronos.chronograph.internal.impl.util.ChronoGraphLoggingUtil;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.chronos.chronograph.internal.impl.util.PredefinedProperty;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronosLogMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ChronoEdgeImpl extends AbstractChronoElement implements Edge, ChronoEdge {

    private static final Logger log = LoggerFactory.getLogger(ChronoEdgeImpl.class);

    public static ChronoEdgeImpl create(final ChronoGraphInternal graph, final ChronoGraphTransactionInternal tx,
                                        final IEdgeRecord record) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
        String id = record.getId();
        String outV = record.getOutVertexId();
        String label = record.getLabel();
        String inV = record.getInVertexId();
        Set<? extends IPropertyRecord> properties = record.getProperties();
        ChronoEdgeImpl edge = new ChronoEdgeImpl(graph, tx, id, outV, label, inV, properties);
        edge.updateLifecycleStatus(ElementLifecycleEvent.SAVED);
        return edge;
    }

    public static ChronoEdgeImpl create(final String id, final ChronoVertexImpl outV, final String label,
                                        final ChronoVertexImpl inV) {
        checkNotNull(outV, "Precondition violation - argument 'outV' must not be NULL!");
        ElementHelper.validateLabel(label);
        checkNotNull(inV, "Precondition violation - argument 'inV' must not be NULL!");
        ChronoEdgeImpl edge = new ChronoEdgeImpl(id, outV, label, inV);
        edge.updateLifecycleStatus(ElementLifecycleEvent.CREATED);
        return edge;
    }

    public static ChronoEdgeImpl incomingEdgeFromRecord(final ChronoVertexImpl owner, final String label,
                                                        final IEdgeTargetRecord record) {
        checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
        checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
        checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
        String id = record.getEdgeId();
        String otherEndVertexId = record.getOtherEndVertexId();
        ChronoEdgeImpl edge = new ChronoEdgeImpl(id, label, owner, otherEndVertexId, owner.id(),
            ElementLifecycleEvent.RELOADED_FROM_DB_AND_IN_SYNC);
        // tell the edge that it's properties must be loaded from the backing data store on first access
        edge.lazyLoadProperties = true;
        return edge;
    }

    public static ChronoEdgeImpl outgoingEdgeFromRecord(final ChronoVertexImpl owner, final String label,
                                                        final IEdgeTargetRecord record) {
        checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
        checkNotNull(record, "Precondition violation - argument 'record' must not be NULL!");
        checkNotNull(label, "Precondition violation - argument 'label' must not be NULL!");
        String id = record.getEdgeId();
        String otherEndVertexId = record.getOtherEndVertexId();
        ChronoEdgeImpl edge = new ChronoEdgeImpl(id, label, owner, owner.id(), otherEndVertexId,
            ElementLifecycleEvent.RELOADED_FROM_DB_AND_IN_SYNC);
        // tell the edge that it's properties must be loaded from the backing data store on first access
        edge.lazyLoadProperties = true;
        return edge;
    }

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final String outVid;
    private final String inVid;
    protected final Map<String, ChronoProperty<?>> properties;

    private transient WeakReference<ChronoVertex> outVcache;
    private transient WeakReference<ChronoVertex> inVcache;
    private transient boolean lazyLoadProperties;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected ChronoEdgeImpl(final ChronoGraphInternal graph, final ChronoGraphTransactionInternal tx, final String id,
                             final String outVid, final String label, final String inVid, final Set<? extends IPropertyRecord> properties) {
        super(graph, tx, id, label);
        checkNotNull(outVid, "Precondition violation - argument 'outVid' must not be NULL!");
        checkNotNull(inVid, "Precondition violation - argument 'inVid' must not be NULL!");
        this.outVid = outVid;
        this.inVid = inVid;
        this.properties = Maps.newHashMap();
        for (IPropertyRecord pRecord : properties) {
            String propertyName = pRecord.getKey();
            Object propertyValue = pRecord.getValue();
            this.properties.put(propertyName, new ChronoProperty(this, propertyName, propertyValue, true));
        }
    }

    protected ChronoEdgeImpl(final String id, final String label, final ChronoVertexImpl owner, final String outVid,
                             final String inVid, final ElementLifecycleEvent event) {
        super(owner.graph(), owner.getOwningTransaction(), id, label);
        checkNotNull(outVid, "Precondition violation - argument 'outVid' must not be NULL!");
        checkNotNull(inVid, "Precondition violation - argument 'inVid' must not be NULL!");
        checkNotNull(owner, "Precondition violation - argument 'owner' must not be NULL!");
        this.outVid = outVid;
        this.inVid = inVid;
        this.properties = Maps.newHashMap();
        if (owner.id().equals(outVid)) {
            this.outVcache = new WeakReference<>(owner);
        } else if (owner.id().equals(inVid)) {
            this.inVcache = new WeakReference<>(owner);
        } else {
            throw new IllegalArgumentException("The given owner is neither the in-vertex nor the out-vertex!");
        }
        this.updateLifecycleStatus(event);
    }

    protected ChronoEdgeImpl(final String id, final ChronoVertexImpl outV, final String label,
                             final ChronoVertexImpl inV) {
        super(inV.graph(), inV.getGraphTransaction(), id, label);
        checkNotNull(outV, "Precondition violation - argument 'outV' must not be NULL!");
        checkNotNull(inV, "Precondition violation - argument 'inV' must not be NULL!");
        // we need to check the access on the vertices, as we might have to transition them to another transaction
        outV.checkAccess();
        inV.checkAccess();
        if (outV.getOwningTransaction().equals(inV.getOwningTransaction()) == false) {
            throw new IllegalArgumentException("The given vertices are bound to different transactions!");
        }
        if (outV.getOwningThread().equals(Thread.currentThread()) == false
            || inV.getOwningThread().equals(Thread.currentThread()) == false) {
            throw new IllegalStateException("Cannot create edge - neighboring vertices belong to different threads!");
        }
        this.outVid = outV.id();
        this.inVid = inV.id();
        this.properties = Maps.newHashMap();
        this.inVcache = new WeakReference<>(inV);
        this.outVcache = new WeakReference<>(outV);
    }

    // =================================================================================================================
    // TINKERPOP 3 API
    // =================================================================================================================

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        this.checkAccess();
        checkNotNull(direction, "Precondition violation - argument 'direction' must not be NULL!");
        switch (direction) {
            case BOTH:
                // note: according to gremlin specification, the out vertex should be returned first.
                return Iterators.forArray(this.outVertex(), this.inVertex());
            case IN:
                return Iterators.forArray(this.inVertex());
            case OUT:
                return Iterators.forArray(this.outVertex());
            default:
                throw new IllegalArgumentException("Unkown 'Direction' literal: " + direction);
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        if(value == null){
            // Since Gremlin 3.5.2: setting a property to value NULL removes it.
            this.removeProperty(key);
            return Property.empty();
        }
        this.checkAccess();
        this.loadLazyPropertiesIfRequired();
        this.logPropertyChange(key, value);
        boolean exists = this.property(key).isPresent();
        ChronoProperty<V> newProperty = new ChronoProperty<>(this, key, value);
        if (exists) {
            this.changePropertyStatus(key, PropertyStatus.MODIFIED);
        } else {
            this.changePropertyStatus(key, PropertyStatus.NEW);
        }
        this.properties.put(key, newProperty);
        this.updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
        return newProperty;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        this.checkAccess();
        // Since Gremlin 3.5.2: querying properties(null) is now allowed and returns the empty iterator.
        if(propertyKeys != null && propertyKeys.length > 0 && Arrays.stream(propertyKeys).allMatch(Objects::isNull)){
            return Collections.emptyIterator();
        }
        this.loadLazyPropertiesIfRequired();
        Set<Property> matchingProperties = Sets.newHashSet();
        if (propertyKeys == null || propertyKeys.length <= 0) {
            // note: the TinkerPop test suite explicitly demands that predefined property keys,
            // such as T.id, T.label etc. are EXCLUDED from the iterator in this case.
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
    public ChronoVertex inVertex() {
        this.checkAccess();
        ChronoVertex inVertex = null;
        if (this.inVcache != null) {
            // we have loaded this element once before, see if it's cached
            inVertex = this.inVcache.get();
        }
        if (inVertex == null) {
            // either we have never loaded this element before, or the garbage collector
            // decided to remove our cached instance. In this case, we need to reload it.
            inVertex = this.resolveVertex(this.inVid);
            // remember it in the cache
            this.inVcache = new WeakReference<>(inVertex);
        }
        return inVertex;
    }

    @Override
    public ChronoVertex outVertex() {
        this.checkAccess();
        ChronoVertex outVertex = null;
        if (this.outVcache != null) {
            // we have loaded this element once before, see if it's cached
            outVertex = this.outVcache.get();
        }
        if (outVertex == null) {
            // either we have never loaded this element before, or the garbage collector
            // decided to remove our cached instance. In this case, we need to reload it.
            outVertex = this.resolveVertex(this.outVid);
            // remember it in the cache
            this.outVcache = new WeakReference<>(outVertex);
        }
        return outVertex;
    }

    @Override
    public void remove() {
        this.checkThread();
        this.checkTransaction();
        if(this.isRemoved()){
            // removing an edge twice has no effect (as defined in Gremlin standard)
            return;
        }
        this.logEdgeRemove();
        this.withoutRemovedCheck(() -> {
            super.remove();
            if (this.inVertex().equals(this.outVertex())) {
                // self reference, sufficient to call remove() only on one end
                ChronoProxyUtil.resolveVertexProxy(this.inVertex()).removeEdge(this);
            } else {
                // reference to other vertex, need to remove edge from both ends
                ChronoProxyUtil.resolveVertexProxy(this.inVertex()).removeEdge(this);
                ChronoProxyUtil.resolveVertexProxy(this.outVertex()).removeEdge(this);
            }
        });
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    protected void reloadFromDatabase() {
        // clear the inV and outV caches (but keep the ids, as the vertices connected to an edge can never change)
        ElementLifecycleEvent[] lifecycleEvent = new ElementLifecycleEvent[1];
        this.withoutRemovedCheck(() -> {
            this.withoutModificationCheck(() -> {
                this.inVcache = null;
                this.outVcache = null;
                this.properties.clear();
                ChronoDBTransaction backendTx = this.getOwningTransaction().getBackingDBTransaction();
                EdgeRecord eRecord = backendTx.get(ChronoGraphConstants.KEYSPACE_EDGE, this.id());
                if (eRecord == null) {
                    // edge was removed
                    lifecycleEvent[0] = ElementLifecycleEvent.RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT;
                } else {
                    lifecycleEvent[0] = ElementLifecycleEvent.RELOADED_FROM_DB_AND_IN_SYNC;
                    // load the properties
                    this.lazyLoadProperties = false;
                    for (IPropertyRecord propertyRecord : eRecord.getProperties()) {
                        String key = propertyRecord.getKey();
                        Object value = propertyRecord.getValue();
                        this.property(key, value);
                    }
                }
                this.getTransactionContext().registerLoadedEdge(this);
            });
        });
        this.updateLifecycleStatus(lifecycleEvent[0]);
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    @Override
    public void removeProperty(final String key) {
        this.checkAccess();
        this.loadLazyPropertiesIfRequired();
        this.logPropertyRemove(key);
        this.properties.remove(key);
        this.changePropertyStatus(key, PropertyStatus.REMOVED);
        this.updateLifecycleStatus(ElementLifecycleEvent.PROPERTY_CHANGED);
    }

    @Override
    public void notifyPropertyChanged(final ChronoProperty<?> chronoProperty) {
        // nothing to do for edges so far
    }

    public IEdgeRecord toRecord() {
        String id = this.id();
        String label = this.label();
        this.loadLazyPropertiesIfRequired();
        return new EdgeRecord2(id, this.outVid, label, this.inVid, this.properties);
    }

    @Override
    public void updateLifecycleStatus(final ElementLifecycleEvent event) {
        super.updateLifecycleStatus(event);
        if (this.isModificationCheckActive()) {
            if (event == ElementLifecycleEvent.CREATED || event == ElementLifecycleEvent.DELETED
                || event == ElementLifecycleEvent.RECREATED_FROM_REMOVED || event == ElementLifecycleEvent.RECREATED_FROM_OBSOLETE) {
                // need to update adjacency lists of in and out vertex
                this.inVertex().updateLifecycleStatus(ElementLifecycleEvent.ADJACENT_EDGE_ADDED_OR_REMOVED);
                this.outVertex().updateLifecycleStatus(ElementLifecycleEvent.ADJACENT_EDGE_ADDED_OR_REMOVED);
            }
            if (this.getStatus().isDirty()) {
                this.getTransactionContext().markEdgeAsModified(this);
            }
        }
    }

    @Override
    public void validateGraphInvariant() {
        this.withoutRemovedCheck(() -> {
            switch (this.getStatus()) {
                case PERSISTED:
                    /* fall through */
                case PROPERTY_CHANGED:
                    /* fall through */
                case EDGE_CHANGED:
                    /* fall through */
                case NEW:
                    // the edge exists, check the pointers
                    try {
                        if (this.outVertex().isRemoved()) {
                            throw new GraphInvariantViolationException("The Edge '" + this.id() + "' with Label '" + this.label() + "' references the OUT Vertex '" + this.outVertex().id() + "', but that vertex has been removed!");
                        }
                        if (Iterators.contains(this.outVertex().edges(Direction.OUT, this.label()), this) == false) {
                            throw new GraphInvariantViolationException("The Edge '" + this.id() + "' with Label '" + this.label() + "' references the OUT Vertex '" + this.outVertex().id() + ", but that vertex does not list this edge as outgoing!");
                        }
                    } catch (NoSuchElementException e) {
                        throw new GraphInvariantViolationException("The Edge '" + this.id() + "' with Label '" + this.label() + "' references the OUT Vertex '" + this.outVid + "', but that vertex does not exist in the graph!");
                    }
                    try {
                        if (this.inVertex().isRemoved()) {
                            throw new GraphInvariantViolationException("The Edge '" + this.id() + "' with Label '" + this.label() + "' references the IN Vertex '" + this.inVertex().id() + "', but that vertex has been removed!");
                        }
                        if (Iterators.contains(this.inVertex().edges(Direction.IN, this.label()), this) == false) {
                            throw new GraphInvariantViolationException("The Edge '" + this.id() + "' with Label '" + this.label() + "' references the IN Vertex '" + this.inVertex().id() + "', but that vertex does not list this edge as incoming!");
                        }
                    } catch (NoSuchElementException e) {
                        throw new GraphInvariantViolationException("The Edge '" + this.id() + "' with Label '" + this.label() + "' references the IN Vertex '" + this.inVid + "', but that vertex does not exist in the graph!");
                    }
                    break;
                case OBSOLETE:
                    /* fall through */
                case REMOVED:
                    try {
                        // the edge has been removed, assert that it is no longer referenced by adjacent vertices
                        if (!this.outVertex().isRemoved()) {
                            // out vertex still exists, make sure it does not list this edge as outgoing
                            if (Iterators.contains(this.outVertex().edges(Direction.OUT, this.label()), this)) {
                                throw new GraphInvariantViolationException("The Edge '" + this.id() + "' with Label '" + this.label() + "' has been removed, but is still referenced by its OUT Vertex '" + this.outVertex().id() + "'!");
                            }
                        }
                    } catch (NoSuchElementException expected) {
                        // in this case, we have been unable to resolve the out-vertex, because it has
                        // been deleted and is not present in our cache. This is okay, because if the
                        // out vertex doesn't exist anymore and this edge doesn't exist either, then
                        // there is no inconsistency here.
                    }

                    try {
                        if (!this.inVertex().isRemoved()) {
                            // in vertex still exists, make sure it does not list this edge as incoming
                            if (Iterators.contains(this.inVertex().edges(Direction.IN, this.label()), this)) {
                                throw new GraphInvariantViolationException("The Edge '" + this.id() + "' with Label '" + this.label() + "' has been removed, but is still referenced by its IN Vertex '" + this.inVertex().id() + "'!");
                            }
                        }
                    } catch (NoSuchElementException expected) {
                        // in this case, we have been unable to resolve the out-vertex, because it has
                        // been deleted and is not present in our cache. This is okay, because if the
                        // out vertex doesn't exist anymore and this edge doesn't exist either, then
                        // there is no inconsistency here.
                    }
                    break;
                default:
                    throw new UnknownEnumLiteralException(this.getStatus());
            }


        });
    }

    @Override
    public boolean isLazy() {
        return this.lazyLoadProperties;
    }

    // =====================================================================================================================
    // UTILITY
    // =====================================================================================================================

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void loadLazyPropertiesIfRequired() {
        if (this.lazyLoadProperties == false) {
            // lazy loading of properties is not required
            return;
        }
        ChronoGraphTransaction graphTx = this.getGraphTransaction();
        IEdgeRecord edgeRecord = graphTx.getBackingDBTransaction().get(ChronoGraphConstants.KEYSPACE_EDGE, this.id());
        if (edgeRecord == null) {
            List<Long> history = Lists.newArrayList(Iterators.limit(graphTx.getEdgeHistory(this.id()), 10));
            int vertices = graphTx.getBackingDBTransaction().keySet(ChronoGraphConstants.KEYSPACE_VERTEX).size();
            int edges = graphTx.getBackingDBTransaction().keySet(ChronoGraphConstants.KEYSPACE_EDGE).size();
            throw new IllegalStateException(
                "Failed to load edge properties - there is no backing Edge Record in the database for ID: '"
                    + this.id() + "' at coordinates [" + graphTx.getBranchName() + "@" + graphTx.getTimestamp() + "]! " +
                    "The graph contains " + vertices + " vertices and " + edges + " edges at the specified coordinates. " +
                    "The last 10 commit timestamps on this edge are: " + history);
        }
        // load the properties from the edge record
        for (IPropertyRecord propertyRecord : edgeRecord.getProperties()) {
            String key = propertyRecord.getKey();
            Object value = propertyRecord.getValue();
            ChronoProperty<?> property = new ChronoProperty(this, key, value, true);
            this.properties.put(key, property);
        }
        // remember that properties do not need to be re-loaded
        this.lazyLoadProperties = false;
    }

    // =================================================================================================================
    // DEBUG OUTPUT
    // =================================================================================================================

    private void logPropertyChange(final String key, final Object value) {
        if (!log.isTraceEnabled(ChronosLogMarker.CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(ChronoGraphLoggingUtil.createLogHeader(this.owningTransaction));
        messageBuilder.append("Setting Property '");
        messageBuilder.append(key);
        messageBuilder.append("' ");
        if (this.property(key).isPresent()) {
            messageBuilder.append("from '");
            messageBuilder.append(this.value(key).toString());
            messageBuilder.append("' to '");
            messageBuilder.append(value.toString());
            messageBuilder.append("' ");
        } else {
            messageBuilder.append("to '");
            messageBuilder.append(value.toString());
            messageBuilder.append("' (new property) ");
        }
        messageBuilder.append(" on Edge ");
        messageBuilder.append(this.toString());
        messageBuilder.append(" (Object ID: ");
        messageBuilder.append(System.identityHashCode(this));
        messageBuilder.append(")");
        log.trace(ChronosLogMarker.CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, messageBuilder.toString());
    }

    private void logEdgeRemove() {
        if (!log.isTraceEnabled(ChronosLogMarker.CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(ChronoGraphLoggingUtil.createLogHeader(this.owningTransaction));
        messageBuilder.append("Removing Edge ");
        messageBuilder.append(this);
        messageBuilder.append(" (Object ID: ");
        messageBuilder.append(System.identityHashCode(this));
        messageBuilder.append(")");
        log.trace(ChronosLogMarker.CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, messageBuilder.toString());
    }

    private void logPropertyRemove(final String key) {
        if (!log.isTraceEnabled(ChronosLogMarker.CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS)) {
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(ChronoGraphLoggingUtil.createLogHeader(this.owningTransaction));
        messageBuilder.append("Removing Property '");
        messageBuilder.append(key);
        messageBuilder.append("' from Edge ");
        messageBuilder.append(this);
        messageBuilder.append(" (Object ID: ");
        messageBuilder.append(System.identityHashCode(this));
        messageBuilder.append(")");
        log.trace(ChronosLogMarker.CHRONOS_LOG_MARKER__GRAPH_MODIFICATIONS, messageBuilder.toString());
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private class PropertiesIterator<V> implements Iterator<Property<V>> {

        @SuppressWarnings("rawtypes")
        private final Iterator<Property> iter;

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
        public Property<V> next() {
            Property<V> p = this.iter.next();
            return p;
        }

    }

}
