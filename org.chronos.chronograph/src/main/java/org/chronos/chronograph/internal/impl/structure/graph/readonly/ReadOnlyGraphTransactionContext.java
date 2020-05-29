package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.GraphTransactionContext;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyGraphTransactionContext implements GraphTransactionContext {

    private final GraphTransactionContext context;

    public ReadOnlyGraphTransactionContext(GraphTransactionContext context) {
        checkNotNull(context, "Precondition violation - argument 'context' must not be NULL!");
        this.context = context;
    }

    @Override
    public Set<ChronoVertex> getModifiedVertices() {
        Set<ChronoVertex> vertices = this.context.getModifiedVertices();
        Set<ChronoVertex> result = vertices.stream().map(ReadOnlyChronoVertex::new).collect(Collectors.toSet());
        return Collections.unmodifiableSet(result);
    }

    @Override
    public Set<ChronoEdge> getModifiedEdges() {
        Set<ChronoEdge> edges = this.context.getModifiedEdges();
        Set<ChronoEdge> result = edges.stream().map(ReadOnlyChronoEdge::new).collect(Collectors.toSet());
        return Collections.unmodifiableSet(result);
    }

    @Override
    public Set<String> getModifiedVariables(String keyspace) {
        return Collections.unmodifiableSet(this.context.getModifiedVariables(keyspace));
    }

    @Override
    public Set<ChronoElement> getModifiedElements() {
        Set<ChronoElement> allElements = Sets.newHashSet();
        allElements.addAll(this.getModifiedVertices());
        allElements.addAll(this.getModifiedEdges());
        return Collections.unmodifiableSet(allElements);
    }

    @Override
    public boolean isDirty() {
        return this.context.isDirty();
    }

    @Override
    public boolean isVertexModified(final Vertex vertex) {
        return this.context.isVertexModified(vertex);
    }

    @Override
    public boolean isVertexModified(final String vertexId) {
        return this.context.isVertexModified(vertexId);
    }


    @Override
    public boolean isEdgeModified(final Edge edge) {
        return this.context.isEdgeModified(edge);
    }

    @Override
    public boolean isEdgeModified(final String edgeId) {
        return this.context.isEdgeModified(edgeId);
    }

    @Override
    public boolean isVariableModified(final String keyspace, final String variableName) {
        return this.context.isVariableModified(keyspace, variableName);
    }

    @Override
    public boolean isVariableRemoved(final String keyspace, final String variableName) {
        return this.context.isVariableRemoved(keyspace, variableName);
    }

    @Override
    public Object getModifiedVariableValue(final String keyspace, final String variableName) {
        return this.context.getModifiedVariableValue(keyspace, variableName);
    }

    @Override
    public Set<String> getModifiedVariableKeyspaces() {
        return Collections.unmodifiableSet(this.context.getModifiedVariableKeyspaces());
    }

    @Override
    public Set<String> getRemovedVariables(String keyspace) {
        return Collections.unmodifiableSet(this.context.getRemovedVariables(keyspace));
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("This operation is not supported in a read-only graph!");
    }

    @Override
    public ChronoVertex getModifiedVertex(final String id) {
        ChronoVertex vertex = this.context.getModifiedVertex(id);
        if (vertex == null) {
            return null;
        }
        return new ReadOnlyChronoVertex(vertex);
    }

    @Override
    public ChronoEdge getModifiedEdge(final String id) {
        ChronoEdge edge = this.context.getModifiedEdge(id);
        if (edge == null) {
            return null;
        }
        return new ReadOnlyChronoEdge(edge);
    }
}
