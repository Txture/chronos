package org.chronos.chronograph.internal.api.index;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.impl.index.IndexingOption;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.impl.index.IndexType;

import java.util.Iterator;
import java.util.Set;

/**
 * The internal representation of the {@link ChronoGraphIndexManager} with additional methods for internal use.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphIndexManagerInternal extends ChronoGraphIndexManager {

    // =====================================================================================================================
    // INDEX MANIPULATION
    // =====================================================================================================================

    /**
     * Adds an index on the given property.
     *
     * @param elementType  The type of element ({@link Vertex} or {@link Edge}) to add the index on.
     * @param indexType    The type of value to index.
     * @param propertyName The name of the gremlin property to index.
     * @param startTimestamp The timestamp at which the index should start (inclusive). Must be less than or equal to the "now" timestamp on the branch.
     * @param endTimestamp The timestamp at which the index should end (exclusive). Must be greater than <code>startTimestamp</code>.
     * @param options The indexing options to apply.
     */
    public ChronoGraphIndex addIndex(Class<? extends Element> elementType, IndexType indexType, String propertyName, long startTimestamp, long endTimestamp, Set<IndexingOption> options);

    // =====================================================================================================================
    // INDEX QUERYING
    // =====================================================================================================================

    /**
     * Performs an index search for the vertices that meet <b>all</b> of the given search specifications.
     *
     * @param tx                   The graph transaction to operate on. Must not be <code>null</code>, must be open.
     * @param searchSpecifications The search specifications to find the matching vertices for. Must not be <code>null</code>.
     * @return An iterator over the IDs of all vertices that fulfill all given search specifications. May be empty, but
     * never <code>null</code>.
     */
    public Iterator<String> findVertexIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?, ?>> searchSpecifications);

    /**
     * Performs an index search for the edges that meet <b>all</b> of the given search specifications.
     *
     * @param tx                   The graph transaction to operate on. Must not be <code>null</code>, must be open.
     * @param searchSpecifications The search specifications to find the matching edges for. Must not be <code>null</code>.
     * @return An iterator over the IDs of all edges that fulfill all given search specifications. May be empty, but
     * never <code>null</code>.
     */
    public Iterator<String> findEdgeIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?, ?>> searchSpecifications);


}
