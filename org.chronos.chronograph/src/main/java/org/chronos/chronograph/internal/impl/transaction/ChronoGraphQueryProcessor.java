package org.chronos.chronograph.internal.impl.transaction;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.exceptions.UnknownKeyspaceException;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.GraphTransactionContext;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal;
import org.chronos.chronograph.internal.impl.util.ChronoGraphQueryUtil;
import org.chronos.chronograph.internal.impl.util.ChronoProxyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphQueryProcessor {

    private static final Logger log = LoggerFactory.getLogger(ChronoGraphQueryProcessor.class);

    private final StandardChronoGraphTransaction tx;

    public ChronoGraphQueryProcessor(final StandardChronoGraphTransaction tx) {
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        this.tx = tx;
    }

    public Iterator<Vertex> getAllVerticesIterator() {
        ChronoDBTransaction tx = this.tx.getBackingDBTransaction();
        Set<String> keySet = Sets.newHashSet();
        try {
            keySet = tx.keySet(ChronoGraphConstants.KEYSPACE_VERTEX);
        } catch (UnknownKeyspaceException ignored) {
        }
        GraphTransactionContext context = this.tx.getContext();
        if (context.isDirty() == false) {
            // no transient modifications; return the persistent state directly
            return new VertexResolvingIterator(keySet.iterator(), ElementLoadMode.LAZY);
        }
        // our context is dirty, therefore we have to add all new vertices and remove all deleted vertices
        Set<String> modifiedKeySet = Sets.newHashSet();
        // TODO [Performance] ChronoGraph: refactor this once we have proper "is new" handling for transient vertices
        // check for all vertices if they were removed
        Set<String> removedVertexIds = Sets.newHashSet();
        for (ChronoVertex vertex : context.getModifiedVertices()) {
            String id = vertex.id();
            if (vertex.isRemoved()) {
                removedVertexIds.add(id);
            } else {
                modifiedKeySet.add(id);
            }
        }
        for (String id : keySet) {
            if (removedVertexIds.contains(id) == false) {
                modifiedKeySet.add(id);
            }
        }
        Iterator<Vertex> resultIterator = new VertexResolvingIterator(modifiedKeySet.iterator(), ElementLoadMode.LAZY);
        return ChronoProxyUtil.replaceVerticesByProxies(resultIterator, this.tx);
    }

    public Iterator<Vertex> getVerticesIterator(final Iterable<String> chronoVertexIds,
                                                final ElementLoadMode loadMode) {
        checkNotNull(chronoVertexIds, "Precondition violation - argument 'chronoVertexIds' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        GraphTransactionContext context = this.tx.getContext();
        Set<String> modifiedSet = Sets.newHashSet(chronoVertexIds);
        if (context.isDirty()) {
            // consider deleted vertices
            for (String vertexId : chronoVertexIds) {
                if (context.isVertexModified(vertexId) == false) {
                    // vertex is not modified, keep it
                    modifiedSet.add(vertexId);
                } else {
                    // vertex may have been removed
                    ChronoVertex vertex = context.getModifiedVertex(vertexId);
                    if (vertex.isRemoved() == false) {
                        // vertex still exists, keep it.
                        // We have to add it to the set because it may have been
                        // added during this transaction. If it was just modified,
                        // it will already be in the set and the 'add' operation
                        // is a no-op.
                        modifiedSet.add(vertexId);
                    } else {
                        // vertex was removed, drop it
                        modifiedSet.remove(vertexId);
                    }
                }
            }
        }
        Iterator<Vertex> resultIterator = new VertexResolvingIterator(modifiedSet.iterator(), loadMode);
        return ChronoProxyUtil.replaceVerticesByProxies(resultIterator, this.tx);
    }

    public Iterator<Edge> getAllEdgesIterator() {
        ChronoDBTransaction tx = this.tx.getBackingDBTransaction();
        Set<String> keySet = Sets.newHashSet();
        try {
            keySet = tx.keySet(ChronoGraphConstants.KEYSPACE_EDGE);
        } catch (UnknownKeyspaceException ignored) {
        }
        GraphTransactionContext context = this.tx.getContext();
        if (context.isDirty() == false) {
            // no transient modifications; return the persistent state directly
            return new EdgeResolvingIterator(keySet.iterator(), ElementLoadMode.LAZY);
        }
        // our context is dirty, therefore we have to add all new edges and remove all deleted edges
        Set<String> modifiedKeySet = Sets.newHashSet();
        // TODO [Performance] ChronoGraph: refactor this once we have proper "is new" handling for transient edges
        // check for all edges if they were removed
        Set<String> removedEdgeIds = Sets.newHashSet();
        for (ChronoEdge edge : context.getModifiedEdges()) {
            String id = edge.id();
            if (edge.isRemoved()) {
                removedEdgeIds.add(id);
            } else {
                modifiedKeySet.add(id);
            }
        }
        for (String id : keySet) {
            if (removedEdgeIds.contains(id) == false) {
                modifiedKeySet.add(id);
            }
        }
        Iterator<Edge> edges = new EdgeResolvingIterator(modifiedKeySet.iterator(), ElementLoadMode.LAZY);
        return ChronoProxyUtil.replaceEdgesByProxies(edges, this.tx);
    }

    public Iterator<Edge> getEdgesIterator(final Iterable<String> chronoEdgeIds, ElementLoadMode loadMode) {
        checkNotNull(chronoEdgeIds, "Precondition violation - argument 'chronoEdgeIds' must not be NULL!");
        checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
        GraphTransactionContext context = this.tx.getContext();
        Set<String> modifiedSet = Sets.newHashSet(chronoEdgeIds);
        if (context.isDirty()) {
            // consider deleted edges
            for (String edgeId : chronoEdgeIds) {
                if (context.isEdgeModified(edgeId) == false) {
                    // edge is not modified, keep it
                    modifiedSet.add(edgeId);
                } else {
                    // edge may have been removed
                    ChronoEdge edge = context.getModifiedEdge(edgeId);
                    if (edge.isRemoved() == false) {
                        // edge still exists, keep it.
                        // We have to add it to the set because it may have been
                        // added during this transaction. If it was just modified,
                        // it will already be in the set and the 'add' operation
                        // is a no-op.
                        modifiedSet.add(edgeId);
                    } else {
                        // edge was removed, drop it
                        modifiedSet.remove(edgeId);
                    }
                }
            }
        }
        Iterator<Edge> edges = new EdgeResolvingIterator(modifiedSet.iterator(), loadMode);
        return ChronoProxyUtil.replaceEdgesByProxies(edges, this.tx);
    }

    // =====================================================================================================================
    // INTERNAL HELPER METHODS
    // =====================================================================================================================

    private ChronoGraphIndexManagerInternal getIndexManager() {
        String branchName = this.tx.getBackingDBTransaction().getBranchName();
        return (ChronoGraphIndexManagerInternal) this.tx.getGraph().getIndexManagerOnBranch(branchName);
    }

    // =====================================================================================================================
    // INNER CLASSES
    // =====================================================================================================================

    private class VertexResolvingIterator implements Iterator<Vertex> {

        private final ElementLoadMode loadMode;
        private final Iterator<?> idIterator;

        private Vertex nextVertex;

        private VertexResolvingIterator(final Iterator<?> idIterator, final ElementLoadMode loadMode) {
            checkNotNull(idIterator, "Precondition violation - argument 'idIterator' must not be NULL!");
            checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
            this.idIterator = idIterator;
            this.loadMode = loadMode;
            this.tryResolveNextVertex();
        }

        @Override
        public boolean hasNext() {
            return this.nextVertex != null;
        }

        @Override
        public Vertex next() {
            if (this.nextVertex == null) {
                throw new NoSuchElementException();
            }
            Vertex vertex = this.nextVertex;
            this.tryResolveNextVertex();
            return vertex;
        }

        private void tryResolveNextVertex() {
            while (this.idIterator.hasNext()) {
                // check if we have a vertex for this ID
                Object next = this.idIterator.next();
                String vertexId;
                if (next instanceof String) {
                    vertexId = (String) next;
                } else {
                    vertexId = String.valueOf(next);
                }
                Vertex vertex = ChronoGraphQueryProcessor.this.tx.loadVertex(vertexId, this.loadMode);
                if (vertex != null) {
                    this.nextVertex = vertex;
                    return;
                }
            }
            // we ran out of IDs -> there cannot be a next vertex
            this.nextVertex = null;
        }

    }

    private class EdgeResolvingIterator implements Iterator<Edge> {

        private final Iterator<?> idIterator;
        private final ElementLoadMode loadMode;

        private Edge nextEdge;

        private EdgeResolvingIterator(final Iterator<?> idIterator, ElementLoadMode loadMode) {
            checkNotNull(idIterator, "Precondition violation - argument 'idIterator' must not be NULL!");
            checkNotNull(loadMode, "Precondition violation - argument 'loadMode' must not be NULL!");
            this.idIterator = idIterator;
            this.loadMode = loadMode;
            this.tryResolveNextEdge();
        }

        @Override
        public boolean hasNext() {
            return this.nextEdge != null;
        }

        @Override
        public Edge next() {
            if (this.nextEdge == null) {
                throw new NoSuchElementException();
            }
            Edge edge = this.nextEdge;
            this.tryResolveNextEdge();
            return edge;
        }

        private void tryResolveNextEdge() {
            while (this.idIterator.hasNext()) {
                // check if we have a vertex for this ID
                Object next = this.idIterator.next();
                String edgeId;
                if (next instanceof String) {
                    edgeId = (String) next;
                } else {
                    edgeId = String.valueOf(next);
                }
                Edge edge = ChronoGraphQueryProcessor.this.tx.loadEdge(edgeId, this.loadMode);
                if (edge != null) {
                    this.nextEdge = edge;
                    return;
                }
            }
            // we ran out of IDs -> there cannot be a next edge
            this.nextEdge = null;
        }

    }

    private static class PropertyValueFilterPredicate<V extends Element> implements Predicate<V> {

        private final Set<SearchSpecification<?, ?>> searchSpecifications;

        private PropertyValueFilterPredicate(final Set<SearchSpecification<?, ?>> searchSpecs) {
            checkNotNull(searchSpecs, "Precondition violation - argument 'searchSpecs' must not be NULL!");
            this.searchSpecifications = Sets.newHashSet(searchSpecs);
        }

        @Override
        public boolean test(final V element) {
            ChronoElement chronoElement = (ChronoElement) element;
            if (chronoElement.isRemoved()) {
                // never consider removed elements
                return false;
            }
            for (SearchSpecification<?, ?> searchSpec : this.searchSpecifications) {
                if (element.property(searchSpec.getIndex().getName()).isPresent() == false) {
                    // the property in question is not present, it is NOT possible to make
                    // any decision if it matches the given search criterion or not. In particular,
                    // when the search is negated (e.g. 'not equals'), we decide to have a non-match
                    // for non-existing properties
                    return false;
                }
                Object propertyValue = element.value(searchSpec.getIndex().getName());
                boolean searchSpecApplies = ChronoGraphQueryUtil.searchSpecApplies(searchSpec, propertyValue);
                if (searchSpecApplies == false) {
                    // element failed to pass this filter
                    return false;
                }
            }
            // element passed all filters
            return true;
        }

    }
}
