package org.chronos.chronograph.internal.api.transaction;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.transaction.GraphTransactionContext;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoEdgeProxy;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoVertexProxy;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public interface GraphTransactionContextInternal extends GraphTransactionContext {

    public ChronoVertexImpl getLoadedVertexForId(String id);

    public void registerLoadedVertex(ChronoVertexImpl vertex);

    public ChronoEdgeImpl getLoadedEdgeForId(String id);

    public void registerLoadedEdge(ChronoEdgeImpl edge);

    public void registerVertexProxyInCache(ChronoVertexProxy proxy);

    public void registerEdgeProxyInCache(ChronoEdgeProxy proxy);

    public ChronoVertexProxy getOrCreateVertexProxy(Vertex vertex);

    public ChronoEdgeProxy getOrCreateEdgeProxy(Edge edge);

    public void markVertexAsModified(ChronoVertexImpl vertex);

    public void markEdgeAsModified(ChronoEdgeImpl edge);

    public void markPropertyAsModified(ChronoProperty<?> property);

    public void markPropertyAsDeleted(ChronoProperty<?> property);

    public void removeVariable(String keyspace, String variableName);

    public void setVariableValue(String keyspace, String variableName, Object value);

    public Collection<Property<?>> getModifiedVertexProperties(SearchSpecification<?,?> searchSpecification);

    public Collection<Property<?>> getModifiedEdgeProperties(SearchSpecification<?,?> searchSpecification);

    public Set<Vertex> getVerticesWithModificationsOnProperty(String property);

    public Set<Edge> getEdgesWithModificationsOnProperty(String property);

    public default Set<Vertex> getVerticesWithModificationsOnProperties(Set<String> properties){
        return properties.stream().flatMap(p -> this.getVerticesWithModificationsOnProperty(p).stream()).collect(Collectors.toSet());
    }

    public default Set<Edge> getEdgesWithModificationsOnProperties(Set<String> properties){
        return properties.stream().flatMap(p -> this.getEdgesWithModificationsOnProperty(p).stream()).collect(Collectors.toSet());
    }

}
