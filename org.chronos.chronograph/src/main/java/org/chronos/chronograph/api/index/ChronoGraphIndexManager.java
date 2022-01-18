package org.chronos.chronograph.api.index;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.builder.index.ElementTypeChoiceIndexBuilder;
import org.chronos.chronograph.api.builder.index.FinalizableVertexIndexBuilder;
import org.chronos.chronograph.api.builder.index.IndexBuilderStarter;
import org.chronos.chronograph.api.structure.ChronoGraph;

import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

/**
 * The {@link ChronoGraphIndexManager} is responsible for managing the secondary indices for a {@link ChronoGraph} instance.
 *
 * <p>
 * You can get the instance of your graph by calling {@link ChronoGraph#getIndexManagerOnMaster()}.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphIndexManager {

    // =====================================================================================================================
    // INDEX BUILDING
    // =====================================================================================================================

    /**
     * Starting point for the fluent graph index creation API.
     *
     * <p>
     * Use method chaining on the returned object, and call {@link FinalizableVertexIndexBuilder#build()} on the last builder to create the new index.
     *
     * <p>
     * Adding a new graph index marks that particular index as dirty. When you are done adding all your secondary indices to the graph,
     * call {@link #reindexAll()} in order to build them.
     *
     * @return The next builder in the fluent API, for method chaining. Never <code>null</code>.
     */
    public IndexBuilderStarter create();

    // =====================================================================================================================
    // INDEX METADATA QUERYING
    // =====================================================================================================================

    /**
     * Returns the currently available secondary indices for the given graph element class, at the given timestamp.
     *
     * @param clazz     Either <code>{@link Vertex}.class</code> or <code>{@link Edge}.class</code>. Must not be <code>null</code>.
     * @param timestamp The timestamp to check. Must not be negative.
     * @return The currently available secondary indices for the given graph element class. May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertiesAtTimestamp(long)
     * @see #getIndexedEdgePropertyNamesAtTimestamp(long)
     * @see #getIndexedEdgePropertiesAtTimestamp(long)
     * @see #getIndexedEdgePropertyNamesAtTimestamp(long)
     */
    public Set<ChronoGraphIndex> getIndexedPropertiesAtTimestamp(Class<? extends Element> clazz, long timestamp);

    /**
     * Returns the currently available secondary indices for the given graph element class.
     *
     * @param clazz Either <code>{@link Vertex}.class</code> or <code>{@link Edge}.class</code>. Must not be <code>null</code>.
     * @return The currently available secondary indices for the given graph element class. May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertiesAtAnyPointInTime()
     * @see #getIndexedEdgePropertyNamesAtAnyPointInTime()
     * @see #getIndexedEdgePropertiesAtAnyPointInTime()
     * @see #getIndexedEdgePropertyNamesAtAnyPointInTime()
     */
    public Set<ChronoGraphIndex> getIndexedPropertiesAtAnyPointInTime(Class<? extends Element> clazz);


    /**
     * Returns the currently available secondary indices for vertices, at the given timestamp.
     *
     * @param timestamp The timestamp to check. Must not be negative.
     * @return The currently available secondary indices for vertices. May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertyNamesAtTimestamp(long)
     * @see #getIndexedEdgePropertiesAtTimestamp(long)
     * @see #getIndexedEdgePropertyNamesAtTimestamp(long)
     */
    public default Set<ChronoGraphIndex> getIndexedVertexPropertiesAtTimestamp(long timestamp) {
        return getIndexedPropertiesAtTimestamp(Vertex.class, timestamp);
    }

    /**
     * Returns the currently available secondary indices for vertices.
     *
     * @return The currently available secondary indices for vertices. May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertyNamesAtAnyPointInTime()
     * @see #getIndexedEdgePropertiesAtAnyPointInTime()
     * @see #getIndexedEdgePropertyNamesAtAnyPointInTime()
     */
    public default Set<ChronoGraphIndex> getIndexedVertexPropertiesAtAnyPointInTime() {
        return getIndexedPropertiesAtAnyPointInTime(Vertex.class);
    }

    /**
     * Returns the names (keys) of the vertex properties that are currently part of a secondary index, at the given timestamp.
     *
     * @param timestamp The timestamp to check. Must not be negative.
     * @return The set of indexed vertex property names (keys). May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertiesAtTimestamp(long)
     * @see #getIndexedEdgePropertiesAtTimestamp(long)
     * @see #getIndexedEdgePropertyNamesAtTimestamp(long)
     */
    public default Set<String> getIndexedVertexPropertyNamesAtTimestamp(long timestamp) {
        Set<ChronoGraphIndex> indices = this.getIndexedVertexPropertiesAtTimestamp(timestamp);
        return indices.stream().map(ChronoGraphIndex::getIndexedProperty).collect(Collectors.toSet());
    }

    /**
     * Returns the names (keys) of the vertex properties that are currently part of a secondary index.
     *
     * @return The set of indexed vertex property names (keys). May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertiesAtAnyPointInTime()
     * @see #getIndexedEdgePropertiesAtAnyPointInTime()
     * @see #getIndexedEdgePropertyNamesAtAnyPointInTime()
     */
    public default Set<String> getIndexedVertexPropertyNamesAtAnyPointInTime() {
        Set<ChronoGraphIndex> indices = this.getIndexedVertexPropertiesAtAnyPointInTime();
        return indices.stream().map(ChronoGraphIndex::getIndexedProperty).collect(Collectors.toSet());
    }

    /**
     * Returns the currently available secondary indices for edges, at the given timestamp.
     *
     * @param timestamp The timestamp to check. Must not be negative.
     * @return The currently available secondary indices for edges. May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertiesAtTimestamp(long)
     * @see #getIndexedVertexPropertyNamesAtTimestamp(long)
     * @see #getIndexedEdgePropertyNamesAtTimestamp(long)
     */
    public default Set<ChronoGraphIndex> getIndexedEdgePropertiesAtTimestamp(long timestamp) {
        return getIndexedPropertiesAtTimestamp(Edge.class, timestamp);
    }

    /**
     * Returns the currently available secondary indices for edges.
     *
     * @return The currently available secondary indices for edges. May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertiesAtAnyPointInTime()
     * @see #getIndexedVertexPropertyNamesAtAnyPointInTime()
     * @see #getIndexedEdgePropertyNamesAtAnyPointInTime()
     */
    public default Set<ChronoGraphIndex> getIndexedEdgePropertiesAtAnyPointInTime() {
        return getIndexedPropertiesAtAnyPointInTime(Edge.class);
    }

    /**
     * Returns the names (keys) of the edge properties that are currently part of a secondary index, at the given timestamp.
     *
     * @param timestamp The timestamp to check. Must not be negative.
     * @return The set of indexed edge property names (keys). May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertiesAtTimestamp(long)
     * @see #getIndexedVertexPropertyNamesAtTimestamp(long)
     * @see #getIndexedEdgePropertiesAtTimestamp(long)
     */
    public default Set<String> getIndexedEdgePropertyNamesAtTimestamp(long timestamp) {
        Set<ChronoGraphIndex> indices = this.getIndexedEdgePropertiesAtTimestamp(timestamp);
        return indices.stream().map(ChronoGraphIndex::getIndexedProperty).collect(Collectors.toSet());
    }

    /**
     * Returns the names (keys) of the edge properties that are currently part of a secondary index.
     *
     * @return The set of indexed edge property names (keys). May be empty, but never <code>null</code>.
     * @see #getIndexedVertexPropertiesAtAnyPointInTime()
     * @see #getIndexedVertexPropertyNamesAtAnyPointInTime()
     * @see #getIndexedEdgePropertiesAtAnyPointInTime()
     */
    public default Set<String> getIndexedEdgePropertyNamesAtAnyPointInTime() {
        Set<ChronoGraphIndex> indices = this.getIndexedEdgePropertiesAtAnyPointInTime();
        return indices.stream().map(ChronoGraphIndex::getIndexedProperty).collect(Collectors.toSet());
    }

    /**
     * Returns the set of all currently available secondary graph indices.
     *
     * @return The set of all secondary graph indices. May be empty, but never <code>null</code>.
     */
    public Set<ChronoGraphIndex> getAllIndicesAtAnyPointInTime();

    /**
     * Returns the set of all currently available secondary graph indices, at the given timestamp.
     *
     * @param timestamp The timestamp to check. Must not be negative.
     * @return The set of known indices at the given timestamp.
     */
    public Set<ChronoGraphIndex> getAllIndicesAtTimestamp(long timestamp);

    /**
     * Returns the vertex index for the given property name (key) at the given timestamp.
     *
     * @param indexedPropertyName The name (key) of the vertex property to get the secondary index for. Must not be <code>null</code>.
     * @param timestamp           The timestamp to check. Must not be negative.
     * @return The secondary index for the given property, or <code>null</code> if the property is not indexed.
     */
    public ChronoGraphIndex getVertexIndexAtTimestamp(final String indexedPropertyName, long timestamp);

    /**
     * Returns the vertex indices for the given property name (key) over time.
     *
     * @param indexedPropertyName The name (key) of the vertex property to get the secondary index for. Must not be <code>null</code>.
     * @return The secondary indices for the given property over time. May be empty but never <code>null</code>.
     */
    public Set<ChronoGraphIndex> getVertexIndicesAtAnyPointInTime(final String indexedPropertyName);


    /**
     * Returns the edge index for the given property name (key) at the given timestamp.
     *
     * @param indexedPropertyName The name (key) of the edge property to get the secondary index for. Must not be <code>null</code>.
     * @param timestamp           The timestamp to check. Must not be negative.
     * @return The secondary index for the given property, or <code>null</code> if the property is not indexed.
     */
    public ChronoGraphIndex getEdgeIndexAtTimestamp(final String indexedPropertyName, long timestamp);

    /**
     * Returns the edge indices for the given property name (key) over time.
     *
     * @param indexedPropertyName The name (key) of the edge property to get the secondary index for. Must not be <code>null</code>.
     * @return The secondary indices for the given property over time. May be empty but never <code>null</code>.
     */
    public Set<ChronoGraphIndex> getEdgeIndicesAtAnyPointInTime(final String indexedPropertyName);

    /**
     * Checks if the given property name (key) is indexed for the given graph element class, at the given timestamp.
     *
     * @param clazz     The graph element class to check (either <code>{@link Vertex}.class</code> or <code>{@link Edge}.class</code>). Must not be <code>null</code>.
     * @param property  The name (key) of the property to check. Must not be <code>null</code>.
     * @param timestamp The timestamp to check. Must not be negative.
     * @return <code>true</code> if the given property is indexed for the given graph element class and timestamp, otherwise <code>false</code>.
     * @see #isVertexPropertyIndexedAtTimestamp(String, long)
     * @see #isEdgePropertyIndexedAtTimestamp(String, long)
     */
    public default boolean isPropertyIndexedAtTimestamp(final Class<? extends Element> clazz, final String property, long timestamp) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        if (Vertex.class.isAssignableFrom(clazz)) {
            ChronoGraphIndex index = this.getVertexIndexAtTimestamp(property, timestamp);
            return index != null;
        } else if (Edge.class.isAssignableFrom(clazz)) {
            ChronoGraphIndex index = this.getEdgeIndexAtTimestamp(property, timestamp);
            return index != null;
        } else {
            throw new IllegalArgumentException("Unknown graph element class: '" + clazz.getName() + "'!");
        }
    }

    /**
     * Checks if the given property name (key) is indexed for the given graph element class at any point in time.
     *
     * @param clazz    The graph element class to check (either <code>{@link Vertex}.class</code> or <code>{@link Edge}.class</code>). Must not be <code>null</code>.
     * @param property The name (key) of the property to check. Must not be <code>null</code>.
     * @return <code>true</code> if the given property is indexed for the given graph element class and timestamp, otherwise <code>false</code>.
     * @see #isVertexPropertyIndexedAtAnyPointInTime(String)
     * @see #isEdgePropertyIndexedAtAnyPointInTime(String)
     */
    public default boolean isPropertyIndexedAtAnyPointInTime(final Class<? extends Element> clazz, final String property) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        if (Vertex.class.isAssignableFrom(clazz)) {
            return !this.getVertexIndicesAtAnyPointInTime(property).isEmpty();
        } else if (Edge.class.isAssignableFrom(clazz)) {
            return !this.getEdgeIndicesAtAnyPointInTime(property).isEmpty();
        } else {
            throw new IllegalArgumentException("Unknown graph element class: '" + clazz.getName() + "'!");
        }
    }


    /**
     * Checks if the given {@link Vertex} property name (key) is indexed or not.
     *
     * @param property The vertex property name (key) to check. Must not be <code>null</code>.
     * @return <code>true</code> if the given property is indexed on vertices, otherwise <code>false</code>.
     * @see #isEdgePropertyIndexedAtAnyPointInTime(String)
     */
    public default boolean isVertexPropertyIndexedAtAnyPointInTime(final String property) {
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        return this.isPropertyIndexedAtAnyPointInTime(Vertex.class, property);
    }

    /**
     * Checks if the given {@link Vertex} property name (key) is indexed or not, at the given timestamp.
     *
     * @param property  The vertex property name (key) to check. Must not be <code>null</code>.
     * @param timestamp The timestamp to check. Must not be negative.
     * @return <code>true</code> if the given property is indexed on vertices, otherwise <code>false</code>.
     * @see #isEdgePropertyIndexedAtTimestamp(String, long)
     */
    public default boolean isVertexPropertyIndexedAtTimestamp(final String property, long timestamp) {
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.isPropertyIndexedAtTimestamp(Vertex.class, property, timestamp);
    }

    /**
     * Checks if the given {@link Edge} property name (key) is indexed or not.
     *
     * @param property  The edge property name (key) to check. Must not be <code>null</code>.
     * @param timestamp The timestamp to check. Must not be negative.
     * @return <code>true</code> if the given property is indexed on edges, otherwise <code>false</code>.
     * @see #isVertexPropertyIndexedAtTimestamp(String, long)
     */
    public default boolean isEdgePropertyIndexedAtTimestamp(final String property, final long timestamp) {
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.isPropertyIndexedAtTimestamp(Edge.class, property, timestamp);
    }


    /**
     * Checks if the given {@link Edge} property name (key) is indexed or not.
     *
     * @param property The edge property name (key) to check. Must not be <code>null</code>.
     * @return <code>true</code> if the given property is indexed on edges, otherwise <code>false</code>.
     * @see #isVertexPropertyIndexedAtAnyPointInTime(String)
     */
    public default boolean isEdgePropertyIndexedAtAnyPointInTime(final String property) {
        checkNotNull(property, "Precondition violation - argument 'property' must not be NULL!");
        return this.isPropertyIndexedAtAnyPointInTime(Edge.class, property);
    }

    // =====================================================================================================================
    // INDEX CONTENT MANIPULATION
    // =====================================================================================================================

    /**
     * Rebuilds all <i>dirty</i> secondary graph indices from scratch.
     *
     * <p>
     * This operation is mandatory after adding/removing graph indices. Depending on the backend and size of the database, this operation may take considerable amounts of time and should be used with care.
     * </p>
     *
     * <p>
     * To force a rebuild of <i>all</i> secondary graph indices, use {@link #reindexAll(boolean)}.
     * </p>
     */
    public default void reindexAll() {
        this.reindexAll(false);
    }

    /**
     * Rebuilds the secondary graph indices from scratch.
     *
     * <p>
     * This operation is mandatory after adding/removing graph indices. Depending on the backend and size of the database, this operation may take considerable amounts of time and should be used with care.
     * </p>
     *
     * @param force Set to <code>true</code> to reindex <i>all</i> indices, or to <code>false</code> to reindex only <i>dirty</i> indices.
     */
    public void reindexAll(boolean force);

    /**
     * Drops the given graph index.
     *
     * <p>
     * This operation cannot be undone. Use with care.
     *
     * @param index The index to drop. Must not be <code>null</code>.
     */
    public void dropIndex(ChronoGraphIndex index);

    /**
     * Drops all graph indices.
     *
     * <p>
     * This operation cannot be undone. Use with care.
     *
     * @param commitMetadata The metadata for the commit of dropping all indices. May be <code>null</code>.
     */
    public void dropAllIndices(Object commitMetadata);

    /**
     * Drops all graph indices.
     *
     * <p>
     * This operation cannot be undone. Use with care.
     */
    public void dropAllIndices();

    /**
     * Terminates the indexing for the given index.
     *
     * The index will receive the given timestamp as upper bound and will
     * receive no further updates. Any timestamp larger than the given one
     * will not have this index available for querying anymore.
     *
     * @param index     The index to terminate.
     * @param timestamp The timestamp at which to terminate the index.
     * @return the updated index.
     * @throws IllegalArgumentException if the timestamp is invalid (e.g. if it is less than the start timestamp of the index).
     * @throws IllegalStateException    if the index doesn't exist or has already been terminated.
     */
    public ChronoGraphIndex terminateIndex(ChronoGraphIndex index, long timestamp);

    /**
     * Checks if {@linkplain #reindexAll() reindexing} is required or not.
     *
     * <p>
     * Reindexing is required if at least one graph index is dirty.
     *
     * @return <code>true</code> if at least one graph index requires rebuilding, otherwise <code>false</code>.
     */
    public boolean isReindexingRequired();

    /**
     * Returns the set of secondary graph indices that are currently dirty, i.e. require re-indexing.
     *
     * @return The set of dirty secondary indices. May be empty, but never <code>null</code>.
     */
    public default Set<ChronoGraphIndex> getDirtyIndicesAtAnyPointInTime() {
        Set<ChronoGraphIndex> indices = Sets.newHashSet(this.getAllIndicesAtAnyPointInTime());
        indices.removeIf(idx -> !idx.isDirty());
        return indices;
    }

    /**
     * Returns the set of secondary graph indices that are currently dirty (i.e. require re-indexing) at the given timestamp.
     *
     * @param timestamp The timestamp to check. Must not be negative.
     * @return The set of dirty secondary indices affecting the given timestamp. May be empty, but never <code>null</code>.
     */
    public default Set<ChronoGraphIndex> getDirtyIndicesAtTimestamp(long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        Set<ChronoGraphIndex> indices = this.getDirtyIndicesAtAnyPointInTime();
        indices.removeIf(idx -> !idx.getValidPeriod().contains(timestamp));
        return indices;
    }

    /**
     * Returns the indices which are known and currently clean.
     *
     * @return The set of all indices, without the ones which are currently dirty. May be empty.
     */
    public default Set<ChronoGraphIndex> getCleanIndicesAtAnyPointInTime() {
        Set<ChronoGraphIndex> indices = Sets.newHashSet(this.getAllIndicesAtAnyPointInTime());
        indices.removeIf(ChronoGraphIndex::isDirty);
        return indices;
    }

    /**
     * Returns the indices which are known and currently clean at the given timestamp.
     *
     * @param timestamp The timestamp to check. Must not be negative.
     * @return The set of all indices, without the ones which are currently dirty. May be empty.
     */
    public default Set<ChronoGraphIndex> getCleanIndicesAtTimestamp(long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        Set<ChronoGraphIndex> indices = Sets.newHashSet(this.getAllIndicesAtTimestamp(timestamp));
        indices.removeIf(ChronoGraphIndex::isDirty);
        return indices;
    }

}
