package org.chronos.chronograph.api.transaction;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;

import java.util.Set;

public interface GraphTransactionContext {


    public Set<ChronoVertex> getModifiedVertices();

    public Set<ChronoEdge> getModifiedEdges();

    public Set<String> getModifiedVariables(String keyspace);

    public Set<ChronoElement> getModifiedElements();

    public boolean isDirty();

    public boolean isVertexModified(Vertex vertex);

    public boolean isVertexModified(String vertexId);

    public boolean isEdgeModified(Edge edge);

    public boolean isEdgeModified(String edgeId);

    public boolean isVariableModified(String keyspace, String variableName);

    public boolean isVariableRemoved(String keyspace, String variableName);

    public Object getModifiedVariableValue(String keyspace, String variableName);

    public Set<String> getModifiedVariableKeyspaces();

    public Set<String> getRemovedVariables(String keyspace);

    public void clear();

    public ChronoVertex getModifiedVertex(String id);

    public ChronoEdge getModifiedEdge(String id);
}
