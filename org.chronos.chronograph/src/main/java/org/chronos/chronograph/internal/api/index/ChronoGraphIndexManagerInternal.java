package org.chronos.chronograph.internal.api.index;

import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;

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
     * Adds the given graph index to this manager.
     *
     * @param index The index to add. Must not be <code>null</code>.
     */
    public void addIndex(ChronoGraphIndex index);

    /**
     * Adds the given graph index to this manager.
     *
     * @param index The index to add. Must not be <code>null</code>.
     * @param commitMetadata The metadata for the commit of adding an index. May be <code>null</code>.
     */
    public void addIndex(ChronoGraphIndex index, Object commitMetadata);


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
    public Iterator<String> findVertexIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?,?>> searchSpecifications);

    /**
     * Performs an index search for the edges that meet <b>all</b> of the given search specifications.
     *
     * @param tx                   The graph transaction to operate on. Must not be <code>null</code>, must be open.
     * @param searchSpecifications The search specifications to find the matching edges for. Must not be <code>null</code>.
     * @return An iterator over the IDs of all edges that fulfill all given search specifications. May be empty, but
     * never <code>null</code>.
     */
    public Iterator<String> findEdgeIdsByIndexedProperties(final ChronoGraphTransaction tx, final Set<SearchSpecification<?,?>> searchSpecifications);


}
