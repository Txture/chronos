package org.chronos.chronograph.api.history;

import java.util.Collections;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

public interface ChronoGraphHistoryManager {

    /**
     * Restores the given edges and vertices back to their original state as of the given timestamp.
     *
     * <p>
     * <b>This method requires an open graph transaction, and WILL modify its state!</b> No commit will be performed.
     * </p>
     *
     * <p>
     * During the restoration process, any state of the current element will be <b>overwritten</b> by the historical state!
     * </p>
     *
     * @param timestamp The timestamp to revert back to. Must be less than or equal to the current transaction timestamp, and must not be negative.
     * @param vertexIds The vertex IDs to restore from the given timestamp. May be <code>null</code> or empty (implying that no vertices should be restored). IDs which cannot be found will be treated as "did not exist in the restore timestamp state" and will be deleted in the current graph transaction. Please note that restoring a vertex will also restore its adjacent edges, if possible.
     * @param edgeIds   The edge IDs to restore from the given timestamp. May be <code>null</code> or empty (implying that no edges should be restored). IDs which cannot be found will be treated as "did not exist in the restore timestamp state" and will be deleted in the current graph transaction. Please note that restoring an edge does NOT restore its adjacent vertices, thus restoration of edges may fail.
     * @return The restore result, containing further information about the outcome of this operation. Never <code>null</code>. As a side effect, the current graph transaction state will be modified by this operation.
     */
    public RestoreResult restoreGraphElementsAsOf(long timestamp, Set<String> vertexIds, Set<String> edgeIds);

    /**
     * Restores the given vertices back to their original state as of the given timestamp.
     *
     * <p>
     * <b>This method requires an open graph transaction, and WILL modify its state!</b> No commit will be performed.
     * </p>
     *
     * <p>
     * During the restoration process, any state of the current element will be <b>overwritten</b> by the historical state!
     * </p>
     *
     * @param timestamp The timestamp to revert back to. Must be less than or equal to the current transaction timestamp, and must not be negative.
     * @param vertexIds The vertex IDs to restore from the given timestamp. May be <code>null</code> or empty (implying that no vertices should be restored). IDs which cannot be found will be treated as "did not exist in the restore timestamp state" and will be deleted in the current graph transaction. Please note that restoring a vertex will also restore its adjacent edges, if possible.
     * @return The restore result, containing further information about the outcome of this operation. Never <code>null</code>. As a side effect, the current graph transaction state will be modified by this operation.
     */
    public default RestoreResult restoreVerticesAsOf(long timestamp, Set<String> vertexIds) {
        checkNotNull(vertexIds, "Precondition violation - argument 'vertexIds' must not be NULL!");
        return this.restoreGraphElementsAsOf(timestamp, vertexIds, Collections.emptySet());
    }

    /**
     * Restores the given vertex back to its original state as of the given timestamp.
     *
     * <p>
     * <b>This method requires an open graph transaction, and WILL modify its state!</b> No commit will be performed.
     * </p>
     *
     * <p>
     * During the restoration process, any state of the current element will be <b>overwritten</b> by the historical state!
     * </p>
     *
     * @param timestamp The timestamp to revert back to. Must be less than or equal to the current transaction timestamp, and must not be negative.
     * @param vertexId The vertex ID to restore from the given timestamp. Must not be <code>null</code>. IDs which cannot be found will be treated as "did not exist in the restore timestamp state" and will be deleted in the current graph transaction. Please note that restoring a vertex will also restore its adjacent edges, if possible.
     * @return The restore result, containing further information about the outcome of this operation. Never <code>null</code>. As a side effect, the current graph transaction state will be modified by this operation.
     */
    public default RestoreResult restoreVertexAsOf(long timestamp, String vertexId){
        checkNotNull(vertexId, "Precondition violation - argument 'vertexId' must not be NULL!");
        return this.restoreVerticesAsOf(timestamp, Collections.singleton(vertexId));
    }

    /**
     * Restores the given edges back to their original state as of the given timestamp.
     *
     * <p>
     * <b>This method requires an open graph transaction, and WILL modify its state!</b> No commit will be performed.
     * </p>
     *
     * <p>
     * During the restoration process, any state of the current element will be <b>overwritten</b> by the historical state!
     * </p>
     *
     * @param timestamp The timestamp to revert back to. Must be less than or equal to the current transaction timestamp, and must not be negative.
     * @param edgeIds   The edge IDs to restore from the given timestamp. May be <code>null</code> or empty (implying that no edges should be restored). IDs which cannot be found will be treated as "did not exist in the restore timestamp state" and will be deleted in the current graph transaction. Please note that restoring an edge does NOT restore its adjacent vertices, thus restoration of edges may fail.
     * @return The restore result, containing further information about the outcome of this operation. Never <code>null</code>. As a side effect, the current graph transaction state will be modified by this operation.
     */
    public default RestoreResult restoreEdgesAsOf(long timestamp, Set<String> edgeIds) {
        checkNotNull(edgeIds, "Precondition violation - argument 'edgeIds' must not be NULL!");
        return this.restoreGraphElementsAsOf(timestamp, edgeIds, Collections.emptySet());
    }

    /**
     * Restores the given edge back to its original state as of the given timestamp.
     *
     * <p>
     * <b>This method requires an open graph transaction, and WILL modify its state!</b> No commit will be performed.
     * </p>
     *
     * <p>
     * During the restoration process, any state of the current element will be <b>overwritten</b> by the historical state!
     * </p>
     *
     * @param timestamp The timestamp to revert back to. Must be less than or equal to the current transaction timestamp, and must not be negative.
     * @param edgeId   The edge ID to restore from the given timestamp. Must not be <code>null</code>. IDs which cannot be found will be treated as "did not exist in the restore timestamp state" and will be deleted in the current graph transaction. Please note that restoring an edge does NOT restore its adjacent vertices, thus restoration of edges may fail.
     * @return The restore result, containing further information about the outcome of this operation. Never <code>null</code>. As a side effect, the current graph transaction state will be modified by this operation.
     */
    public default RestoreResult restoreEdgeAsOf(long timestamp, String edgeId){
        checkNotNull(edgeId, "Precondition violation - argument 'edgeId' must not be NULL!");
        return this.restoreEdgesAsOf(timestamp, Collections.singleton(edgeId));
    }

    /**
     * Restores the full graph state as it has been at the given timestamp.
     *
     * <p>
     * <b>This method requires and open graph transaction, and WILL modify its state!</b> No commit will be performed. All changes done previously to this transaction will be lost!
     * </p>
     *
     * @param timestamp The timestamp to revert back to. Must be less than or equal to the current transaction timestamp, and must not be negative.
     * @return The restore result, containing further information about the outcome of this operation. Never <code>null</code>. As a side effect, the current graph transaction state will be modified by this operation.
     */
    public RestoreResult restoreGraphStateAsOf(long timestamp);

}
