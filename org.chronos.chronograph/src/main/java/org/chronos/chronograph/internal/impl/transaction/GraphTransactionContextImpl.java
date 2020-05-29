package org.chronos.chronograph.internal.impl.transaction;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexProperty;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoEdgeProxy;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoVertexProxy;
import org.chronos.chronograph.internal.impl.util.ChronoGraphQueryUtil;
import org.chronos.common.base.CCC;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.logging.LogLevel;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

/**
 * The {@link GraphTransactionContextImpl} keeps track of the elements which have been modified by client code.
 * <p>
 * <p>
 * A TransactionContext is always associated with a {@link ChronoGraphTransaction} that contains the context. The context may be cleared when a {@link ChronoGraphTransaction#rollback()} occurs on the owning transaction.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public class GraphTransactionContextImpl implements GraphTransactionContextInternal {

    private final SetMultimap<String, String> removedVertexPropertyKeyToOwnerId = HashMultimap.create();
    private final SetMultimap<String, String> removedEdgePropertyKeyToOwnerId = HashMultimap.create();
    private final Table<String, String, ChronoProperty<?>> vertexPropertyNameToOwnerIdToModifiedProperty = HashBasedTable.create();
    private final Table<String, String, ChronoProperty<?>> edgePropertyNameToOwnerIdToModifiedProperty = HashBasedTable.create();

    private final Map<String, ChronoVertexImpl> modifiedVertices = Maps.newHashMap();
    private final Map<String, ChronoEdgeImpl> modifiedEdges = Maps.newHashMap();

    private final Map<String, ChronoVertexImpl> loadedVertices = new MapMaker().weakValues().makeMap();
    private final Map<String, ChronoEdgeImpl> loadedEdges = new MapMaker().weakValues().makeMap();

    private final Map<String, ChronoVertexProxy> idToVertexProxy = new MapMaker().weakValues().makeMap();
    private final Map<String, ChronoEdgeProxy> idToEdgeProxy = new MapMaker().weakValues().makeMap();

    private final Map<String, Map<String, Object>> keyspaceToModifiedVariables = Maps.newHashMap();

    // =====================================================================================================================
    // LOADED ELEMENT CACHE API
    // =====================================================================================================================

    @Override
    public ChronoVertexImpl getLoadedVertexForId(final String id) {
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        return this.loadedVertices.get(id);
    }

    @Override
    public void registerLoadedVertex(final ChronoVertexImpl vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        this.loadedVertices.put(vertex.id(), vertex);
        ChronoVertexProxy proxy = this.getWeaklyCachedVertexProxy(vertex.id());
        if (proxy != null) {
            // the proxy already exists; make sure it points to the correct instance
            proxy.rebindTo(vertex);
        }
    }

    @Override
    public ChronoEdgeImpl getLoadedEdgeForId(final String id) {
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        return this.loadedEdges.get(id);
    }

    @Override
    public void registerLoadedEdge(final ChronoEdgeImpl edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        this.loadedEdges.put(edge.id(), edge);
        ChronoEdgeProxy proxy = this.getWeaklyCachedEdgeProxy(edge.id());
        if (proxy != null) {
            // the proxy already exists; make sure it points to the correct instance
            proxy.rebindTo(edge);
        }
    }

    @Override
    public void registerVertexProxyInCache(final ChronoVertexProxy proxy) {
        checkNotNull(proxy, "Precondition violation - argument 'proxy' must not be NULL!");
        this.idToVertexProxy.put(proxy.id(), proxy);
    }

    @Override
    public void registerEdgeProxyInCache(final ChronoEdgeProxy proxy) {
        checkNotNull(proxy, "Precondition violation - argument 'proxy' must not be NULL!");
        this.idToEdgeProxy.put(proxy.id(), proxy);
    }

    /**
     * Returns the {@link ChronoVertexProxy Vertex Proxy} for the {@link Vertex} with the given ID.
     * <p>
     * <p>
     * Bear in mind that proxies are stored in a weak fashion, i.e. they may be garbage collected if they are not referenced anywhere else. This method may and <b>will</b> return <code>null</code> in such cases. Think of this method as a cache.
     * <p>
     * <p>
     * To reliably get a proxy, use {@link #getOrCreateVertexProxy(Vertex)} instead.
     *
     * @param vertexId The ID of the vertex to get the cached proxy for. Must not be <code>null</code>.
     * @return The vertex proxy for the given vertex ID, or <code>null</code> if none is cached.
     */
    private ChronoVertexProxy getWeaklyCachedVertexProxy(final String vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        return this.idToVertexProxy.get(vertexId);
    }

    @Override
    public ChronoVertexProxy getOrCreateVertexProxy(final Vertex vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        if (vertex instanceof ChronoVertexProxy) {
            // already is a proxy
            return (ChronoVertexProxy) vertex;
        }
        // check if we have a proxy
        ChronoVertexProxy proxy = this.getWeaklyCachedVertexProxy((String) vertex.id());
        if (proxy != null) {
            // we already have a proxy; reuse it
            return proxy;
        }
        // we don't have a proxy yet; create one
        proxy = new ChronoVertexProxy((ChronoVertexImpl) vertex);
        this.registerVertexProxyInCache(proxy);
        return proxy;
    }

    /**
     * Returns the {@link ChronoEdgeProxy Edge Proxy} for the {@link Edge} with the given ID.
     * <p>
     * <p>
     * Bear in mind that proxies are stored in a weak fashion, i.e. they may be garbage collected if they are not referenced anywhere else. This method may and <b>will</b> return <code>null</code> in such cases. Think of this method as a cache.
     * <p>
     * <p>
     * To reliably get a proxy, use {@link #getOrCreateEdgeProxy(Edge)} instead.
     *
     * @param edgeId The ID of the vertex to get the cached proxy for. Must not be <code>null</code>.
     * @return The edge proxy for the given vertex ID, or <code>null</code> if none is cached.
     */
    private ChronoEdgeProxy getWeaklyCachedEdgeProxy(final String edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        return this.idToEdgeProxy.get(edgeId);
    }

    @Override
    public ChronoEdgeProxy getOrCreateEdgeProxy(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        if (edge instanceof ChronoEdgeProxy) {
            // already is a proxy
            return (ChronoEdgeProxy) edge;
        }
        // check if we have a proxy
        ChronoEdgeProxy proxy = this.getWeaklyCachedEdgeProxy((String) edge.id());
        if (proxy == null) {
            // we don't have a proxy yet; create one
            proxy = new ChronoEdgeProxy((ChronoEdgeImpl) edge);
        }
        this.registerEdgeProxyInCache(proxy);
        return proxy;
    }

    // =====================================================================================================================
    // MODIFICATION (IS-DIRTY) API
    // =====================================================================================================================

    @Override
    public Set<ChronoVertex> getModifiedVertices() {
        return Collections.unmodifiableSet(Sets.newHashSet(this.modifiedVertices.values()));
    }

    @Override
    public Set<ChronoEdge> getModifiedEdges() {
        return Collections.unmodifiableSet(Sets.newHashSet(this.modifiedEdges.values()));
    }

    @Override
    public Set<String> getModifiedVariables(String keyspace) {
        Map<String, Object> keyspaceModifications = this.keyspaceToModifiedVariables.get(keyspace);
        if (keyspaceModifications == null) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableSet(keyspaceModifications.keySet());
        }
    }

    @Override
    public Set<ChronoElement> getModifiedElements() {
        return Sets.union(this.getModifiedVertices(), this.getModifiedEdges());
    }

    @Override
    public boolean isDirty() {
        return this.modifiedVertices.isEmpty() == false || this.modifiedEdges.isEmpty() == false
            || this.vertexPropertyNameToOwnerIdToModifiedProperty.isEmpty() == false
            || this.edgePropertyNameToOwnerIdToModifiedProperty.isEmpty() == false
            || this.removedVertexPropertyKeyToOwnerId.isEmpty() == false
            || this.removedEdgePropertyKeyToOwnerId.isEmpty() == false
            || this.keyspaceToModifiedVariables.values().stream().allMatch(Map::isEmpty) == false;
    }

    @Override
    public boolean isVertexModified(final Vertex vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        String vertexId = (String) vertex.id();
        return this.isVertexModified(vertexId);
    }

    @Override
    public boolean isVertexModified(final String vertexId) {
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        return this.modifiedVertices.containsKey(vertexId);
    }

    @Override
    public void markVertexAsModified(final ChronoVertexImpl vertex) {
        checkNotNull(vertex, "Precondition violation - argument 'vertex' must not be NULL!");
        this.logMarkVertexAsModified(vertex);
        this.modifiedVertices.put(vertex.id(), vertex);
    }

    @Override
    public boolean isEdgeModified(final Edge edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        String edgeId = (String) edge.id();
        return this.modifiedEdges.containsKey(edgeId);
    }

    @Override
    public boolean isEdgeModified(final String edgeId) {
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        return this.modifiedEdges.containsKey(edgeId);
    }

    @Override
    public void markEdgeAsModified(final ChronoEdgeImpl edge) {
        checkNotNull(edge, "Precondition violation - argument 'edge' must not be NULL!");
        this.logMarkEdgeAsModified(edge);
        this.modifiedEdges.put(edge.id(), edge);
    }

    @Override
    public void markPropertyAsModified(final ChronoProperty<?> property) {
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        this.logMarkPropertyAsModified(property);
        Element owner = property.element();
        String ownerId = (String) owner.id();
        if (owner instanceof Vertex) {
            // mark the property as "modified"
            this.vertexPropertyNameToOwnerIdToModifiedProperty.put(property.key(), ownerId, property);
            // the property is no longer "removed"
            this.removedVertexPropertyKeyToOwnerId.remove(property.key(), ownerId);
        } else if (owner instanceof Edge) {
            // mark the property as "modified"
            this.edgePropertyNameToOwnerIdToModifiedProperty.put(property.key(), ownerId, property);
            // the property is no longer "removed"
            this.removedEdgePropertyKeyToOwnerId.remove(property.key(), ownerId);
        } else if (owner instanceof ChronoVertexProperty) {
            // mark the vertex property itself as modified
            ChronoVertexProperty<?> vp = (ChronoVertexProperty<?>) owner;
            this.vertexPropertyNameToOwnerIdToModifiedProperty.put(vp.key(), ownerId, vp);
        } else {
            throw new IllegalArgumentException("Unknown subclass of Element: " + owner.getClass().getName());
        }
    }

    @Override
    public void markPropertyAsDeleted(final ChronoProperty<?> property) {
        this.logMarkPropertyAsRemoved(property);
        Element owner = property.element();
        String ownerId = (String) owner.id();
        if (owner instanceof Vertex) {
            // the property is no longer "modified"
            this.vertexPropertyNameToOwnerIdToModifiedProperty.remove(property.key(), ownerId);
            // the property now "removed"
            this.removedVertexPropertyKeyToOwnerId.put(property.key(), ownerId);
        } else if (owner instanceof Edge) {
            // the property is no longer "modified"
            this.edgePropertyNameToOwnerIdToModifiedProperty.remove(property.key(), ownerId);
            // the property is now "removed"
            this.removedEdgePropertyKeyToOwnerId.put(property.key(), ownerId);
        } else if (owner instanceof ChronoVertexProperty) {
            // mark the owning property as modified
            ChronoVertexProperty<?> vp = (ChronoVertexProperty<?>) owner;
            this.vertexPropertyNameToOwnerIdToModifiedProperty.put(vp.key(), ownerId, vp);
        } else {
            throw new IllegalArgumentException("Unknown subclass of Element: " + owner.getClass().getName());
        }
    }

    public Set<Vertex> getVerticesWithModificationsOnProperty(String property) {
        Set<String> vertexIds = this.vertexPropertyNameToOwnerIdToModifiedProperty.row(property).keySet();
        return vertexIds.stream().map(this::getModifiedVertex).collect(Collectors.toSet());
    }

    public Set<Edge> getEdgesWithModificationsOnProperty(String property) {
        Set<String> vertexIds = this.edgePropertyNameToOwnerIdToModifiedProperty.row(property).keySet();
        return vertexIds.stream().map(this::getModifiedEdge).collect(Collectors.toSet());
    }

    @Override
    public Collection<Property<?>> getModifiedVertexProperties(final SearchSpecification<?, ?> searchSpec) {
        checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
        // only look at the modified properties that have the property key we are interested in
        String propertyKey = searchSpec.getProperty();
        Map<String, ChronoProperty<?>> modifiedProperties = this.vertexPropertyNameToOwnerIdToModifiedProperty.row(propertyKey);
        // use a list here, because #hashCode and #equals on Properties is defined on key and value only (not on parent
        // element id)
        List<Property<?>> resultList = Lists.newArrayList();
        for (ChronoProperty<?> property : modifiedProperties.values()) {
            Object value = property.value();
            if (ChronoGraphQueryUtil.searchSpecApplies(searchSpec, value)) {
                resultList.add(property);
            }
        }
        return resultList;
    }

    @Override
    public Collection<Property<?>> getModifiedEdgeProperties(final SearchSpecification<?, ?> searchSpec) {
        checkNotNull(searchSpec, "Precondition violation - argument 'searchSpec' must not be NULL!");
        // only look at the modified properties that have the property key we are interested in
        String propertyKey = searchSpec.getProperty();
        Map<String, ChronoProperty<?>> modifiedProperties = this.edgePropertyNameToOwnerIdToModifiedProperty.row(propertyKey);
        // use a list here, because #hashCode and #equals on Properties is defined on key and value only (not on parent
        // element id)
        List<Property<?>> resultList = Lists.newArrayList();
        for (ChronoProperty<?> property : modifiedProperties.values()) {
            Object value = property.value();
            if (ChronoGraphQueryUtil.searchSpecApplies(searchSpec, value)) {
                resultList.add(property);
            }
        }
        return resultList;
    }

    @Override
    public boolean isVariableModified(final String keyspace, final String variableName) {
        checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
        Map<String, Object> keyspaceMap = this.keyspaceToModifiedVariables.get(keyspace);
        if (keyspaceMap == null) {
            return false;
        }
        return keyspaceMap.containsKey(variableName);
    }

    @Override
    public boolean isVariableRemoved(final String keyspace, final String variableName) {
        checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
        Map<String, Object> keyspaceMap = this.keyspaceToModifiedVariables.get(keyspace);
        if (keyspaceMap == null) {
            return false;
        }
        return keyspaceMap.containsKey(variableName) && keyspaceMap.get(variableName) == null;
    }

    @Override
    public Object getModifiedVariableValue(final String keyspace, final String variableName) {
        checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
        Map<String, Object> keyspaceMap = this.keyspaceToModifiedVariables.get(keyspace);
        if (keyspaceMap == null) {
            return null;
        }
        return keyspaceMap.getOrDefault(variableName, null);
    }

    // =====================================================================================================================
    // GRAPH VARIABLES API
    // =====================================================================================================================

    @Override
    public void removeVariable(final String keyspace, final String variableName) {
        checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
        Map<String, Object> keyspaceMap = this.keyspaceToModifiedVariables.computeIfAbsent(keyspace, k -> Maps.newHashMap());
        keyspaceMap.put(variableName, null);
    }

    @Override
    public void setVariableValue(final String keyspace, final String variableName, final Object value) {
        checkNotNull(variableName, "Precondition violation - argument 'variableName' must not be NULL!");
        checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
        Map<String, Object> keyspaceMap = this.keyspaceToModifiedVariables.computeIfAbsent(keyspace, k -> Maps.newHashMap());
        keyspaceMap.put(variableName, value);
    }

    @Override
    public Set<String> getRemovedVariables(String keyspace) {
        Map<String, Object> keyspaceMap = this.keyspaceToModifiedVariables.get(keyspace);
        if (keyspaceMap == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(keyspaceMap.entrySet().stream()
            .filter(entry -> entry.getValue() == null)
            .map(Entry::getKey)
            .collect(Collectors.toSet())
        );
    }

    @Override
    public void clear() {
        this.modifiedVertices.clear();
        this.modifiedEdges.clear();
        this.vertexPropertyNameToOwnerIdToModifiedProperty.clear();
        this.edgePropertyNameToOwnerIdToModifiedProperty.clear();
        this.removedVertexPropertyKeyToOwnerId.clear();
        this.removedEdgePropertyKeyToOwnerId.clear();
        this.keyspaceToModifiedVariables.clear();
        this.loadedVertices.clear();
        this.loadedEdges.clear();
        this.idToVertexProxy.clear();
        this.idToEdgeProxy.clear();
    }

    @Override
    public ChronoVertexImpl getModifiedVertex(final String id) {
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        return this.modifiedVertices.get(id);
    }

    @Override
    public ChronoEdgeImpl getModifiedEdge(final String id) {
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        return this.modifiedEdges.get(id);
    }

    @Override
    public Set<String> getModifiedVariableKeyspaces() {
        return Collections.unmodifiableSet(this.keyspaceToModifiedVariables.entrySet().stream()
            .filter(entry -> entry.getValue() != null)
            .filter(entry -> !entry.getValue().isEmpty())
            .map(Entry::getKey)
            .collect(Collectors.toSet()));
    }

    // =====================================================================================================================
    // DEBUG LOGGING
    // =====================================================================================================================

    private void logMarkVertexAsModified(final ChronoVertex vertex) {
        if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
            // log level is higher than trace, no need to prepare the message
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[GRAPH MODIFICATION] Marking Vertex as modified: ");
        messageBuilder.append(vertex.toString());
        messageBuilder.append(" (Object ID: '");
        messageBuilder.append(System.identityHashCode(vertex));
        messageBuilder.append("). ");
        if (this.modifiedVertices.containsKey(vertex.id())) {
            messageBuilder.append("This Vertex was already marked as modified in this transaction.");
        } else {
            messageBuilder.append("This Vertex has not yet been marked as modified in this transaction.");
        }
        ChronoLogger.logTrace(messageBuilder.toString());
    }

    private void logMarkEdgeAsModified(final ChronoEdge edge) {
        if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
            // log level is higher than trace, no need to prepare the message
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[GRAPH MODIFICATION] Marking Edge as modified: ");
        messageBuilder.append(edge.toString());
        messageBuilder.append(" (Object ID: '");
        messageBuilder.append(System.identityHashCode(edge));
        messageBuilder.append("). ");
        if (this.modifiedEdges.containsKey(edge.id())) {
            messageBuilder.append("This Edge was already marked as modified in this transaction.");
        } else {
            messageBuilder.append("This Edge has not yet been marked as modified in this transaction.");
        }
        ChronoLogger.logTrace(messageBuilder.toString());
    }

    private void logMarkPropertyAsModified(final ChronoProperty<?> property) {
        if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
            // log level is higher than trace, no need to prepare the message
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[GRAPH MODIFICATION] Marking Property as modified: ");
        messageBuilder.append(property.toString());
        messageBuilder.append(" (Object ID: '");
        messageBuilder.append(System.identityHashCode(property));
        messageBuilder.append("). ");
        if (property.isPresent()) {
            ChronoElement owner = property.element();
            messageBuilder.append("Property is owned by: ");
            messageBuilder.append(owner.toString());
            messageBuilder.append(" (Object ID: ");
            messageBuilder.append(System.identityHashCode(owner));
            messageBuilder.append(")");
        }
        ChronoLogger.logTrace(messageBuilder.toString());
    }

    private void logMarkPropertyAsRemoved(final ChronoProperty<?> property) {
        if (CCC.MIN_LOG_LEVEL.isGreaterThan(LogLevel.TRACE)) {
            // log level is higher than trace, no need to prepare the message
            return;
        }
        // prepare some debug output
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[GRAPH MODIFICATION] Marking Property as removed: ");
        messageBuilder.append(property.toString());
        messageBuilder.append(" (Object ID: '");
        messageBuilder.append(System.identityHashCode(property));
        messageBuilder.append("). ");
        if (property.isPresent()) {
            ChronoElement owner = property.element();
            messageBuilder.append("Property is owned by: ");
            messageBuilder.append(owner.toString());
            messageBuilder.append(" (Object ID: ");
            messageBuilder.append(System.identityHashCode(owner));
            messageBuilder.append(")");
        }
        ChronoLogger.logTrace(messageBuilder.toString());
    }
}
