package org.chronos.chronograph.internal.impl.history;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.*;
import org.chronos.chronograph.api.history.ChronoGraphHistoryManager;
import org.chronos.chronograph.api.history.RestoreResult;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphHistoryManagerImpl implements ChronoGraphHistoryManager {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ChronoGraphInternal graph;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ChronoGraphHistoryManagerImpl(ChronoGraphInternal graph) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        this.graph = graph;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public RestoreResult restoreGraphElementsAsOf(final long timestamp, final Set<String> vertexIds, final Set<String> edgeIds) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        ChronoGraphTransaction tx = this.graph.tx().getCurrentTransaction();
        if (tx == null) {
            throw new IllegalStateException("Restoring graph elements requires an open transaction, but currently no transaction is present. Please open a transaction first.");
        }
        long now = tx.getTimestamp();
        if (timestamp > now) {
            throw new IllegalArgumentException("Precondition violation - argument 'timestamp' (value: "
                + timestamp + ") must be less than or equal to the transaction timestamp (value: " + now + ")!");
        }
        if ((vertexIds == null || vertexIds.isEmpty()) && (edgeIds == null || edgeIds.isEmpty())) {
            // nothing to do
            return MutableRestoreResult.empty();
        }
        // prepare the result object
        MutableRestoreResult result = new MutableRestoreResult();
        // open a second transaction to retrieve the state from
        try(ChronoGraph historyGraph = this.graph.tx().createThreadedTx(tx.getBranchName(), timestamp)){
            // prepare the set of edges to restore in addition to the given ones (due to vertex restore)
            Set<String> allEdgeIds = Sets.newHashSet();
            if (edgeIds != null) {
                allEdgeIds.addAll(edgeIds);
            }
            // first restore the vertices (if required)
            if (vertexIds != null && !vertexIds.isEmpty()) {
                allEdgeIds.addAll(restoreVertices(vertexIds, tx, result, historyGraph));
            }
            // then restore the edges (if required)
            if (!allEdgeIds.isEmpty()) {
                restoreEdges(allEdgeIds, tx, result, historyGraph, allEdgeIds);
            }
        }
        return result;
    }



    @Override
    public RestoreResult restoreGraphStateAsOf(final long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        ChronoGraphTransaction tx = this.graph.tx().getCurrentTransaction();
        if (tx == null) {
            throw new IllegalStateException("Restoring graph elements requires an open transaction, but currently no transaction is present. Please open a transaction first.");
        }
        long now = tx.getTimestamp();
        if (timestamp > now) {
            throw new IllegalArgumentException("Precondition violation - argument 'timestamp' (value: "
                + timestamp + ") must be less than or equal to the transaction timestamp (value: " + now + ")!");
        }
        Set<String> vertexIds = Sets.newHashSet();
        Set<String> edgeIds = Sets.newHashSet();
        // collect the differences between the current tx timestamp and the given timestamp
        tx.getGraph().getCommitTimestampsBetween(tx.getBranchName(), timestamp, now).forEachRemaining(commit -> {
            Iterator<String> changedVerticesAtCommit = tx.getGraph().getChangedVerticesAtCommit(tx.getBranchName(), commit);
            Iterators.addAll(vertexIds, changedVerticesAtCommit);
            Iterator<String> changedEdgesAtCommit = tx.getGraph().getChangedEdgesAtCommit(tx.getBranchName(), commit);
            Iterators.addAll(edgeIds, changedEdgesAtCommit);
        });
        // restore everything that has changed since the given timestamp
        return this.restoreGraphElementsAsOf(timestamp, vertexIds, edgeIds);
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private Set<String> restoreVertices(final Set<String> vertexIds, final ChronoGraphTransaction tx, final MutableRestoreResult result, final ChronoGraph historyGraph) {
        Set<String> additionalEdgeIds = Sets.newHashSet();
        Map<String, Vertex> historicalVerticesById = Maps.uniqueIndex(historyGraph.vertices(vertexIds.toArray()), v -> (String) v.id());
        for (String vertexId : vertexIds) {
            // delete the vertex in our current version if it exists
            Vertex currentVertex = Iterators.getOnlyElement(tx.getGraph().vertices(vertexId), null);
            if (currentVertex != null) {
                currentVertex.remove();
            }
            Vertex historicalVertex = historicalVerticesById.get(vertexId);
            if (historicalVertex == null) {
                // vertex did not exist in the previous version, we're done
                result.markVertexAsSuccessfullyRestored(vertexId);
                continue;
            }
            // recreate the vertex
            Vertex newVertex = tx.getGraph().addVertex(
                T.id, historicalVertex.id(),
                T.label, historicalVertex.label()
            );
            historicalVertex.properties().forEachRemaining(vertexProp -> {
                    VertexProperty<?> newProp = newVertex.property(vertexProp.key(), vertexProp.value());
                    // transfer the meta-properties
                    vertexProp.properties().forEachRemaining(metaProp -> newProp.property(metaProp.key(), metaProp.value()));
                }
            );
            // remember to recreate the edges
            historicalVertex.edges(Direction.BOTH).forEachRemaining(e -> additionalEdgeIds.add((String) e.id()));
            result.markVertexAsSuccessfullyRestored(vertexId);
        }
        return additionalEdgeIds;
    }

    private void restoreEdges(final Set<String> edgeIds, final ChronoGraphTransaction tx, final MutableRestoreResult result, final ChronoGraph historyGraph, final Set<String> allEdgeIds) {
        Map<String, Edge> historicalEdgesById = Maps.uniqueIndex(historyGraph.edges(allEdgeIds.toArray()), e -> (String) e.id());
        for (String edgeId : edgeIds) {
            // delete the edge in our current version if it exists
            Edge currentEdge = Iterators.getOnlyElement(tx.getGraph().edges(edgeId), null);
            if (currentEdge != null) {
                currentEdge.remove();
            }
            Edge historicalEdge = historicalEdgesById.get(edgeId);
            if (historicalEdge == null) {
                // edge did not exist in the previous version, we're done
                result.markEdgeAsSuccessfullyRestored(edgeId);
                continue;
            }
            // get the source and target vertices in the current graph
            Vertex inVertex = Iterators.getOnlyElement(tx.getGraph().vertices(historicalEdge.inVertex().id()), null);
            Vertex outVertex = Iterators.getOnlyElement(tx.getGraph().vertices(historicalEdge.outVertex().id()), null);
            if (inVertex == null || outVertex == null) {
                // either source or target does not exist
                result.markEdgeAsFailed(edgeId);
                continue;
            }
            // recreate the edge
            Edge newEdge = outVertex.addEdge(historicalEdge.label(), inVertex);
            // transfer the properties
            historicalEdge.properties().forEachRemaining(prop -> newEdge.property(prop.key(), prop.value()));
            result.markEdgeAsSuccessfullyRestored(edgeId);
        }
    }

}
