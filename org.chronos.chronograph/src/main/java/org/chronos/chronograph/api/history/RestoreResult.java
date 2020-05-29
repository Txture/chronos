package org.chronos.chronograph.api.history;

import java.util.Set;

public interface RestoreResult {

    /**
     * Returns the set of Vertex IDs that were restored successfully.
     *
     * <p>
     * Please note that "restored" can also mean "deleted" if they did not exist at the restoration timestamp.
     * </p>
     *
     * @return The set of Vertex IDs which were restored successfully. May be empty but never <code>null</code>.
     */
    public Set<String> getSuccessfullyRestoredVertexIds();

    /**
     * Returns the set of Edge IDs that were restored successfully.
     *
     * <p>
     * Please note that "restored" can also mean "deleted" if they did not exist at the restoration timestamp.
     * </p>
     *
     * @return The set of Edge IDs which were restored successfully. May be empty but never <code>null</code>.
     */
    public Set<String> getSuccessfullyRestoredEdgeIds();

    /**
     * Returns the set of Edge IDs that could not be restored.
     *
     * <p>
     * This can happen for example if the edge was connected to
     * a vertex which no longer exists.
     * </p>
     *
     * @return The set of edge IDs which failed to be restored. May be empty but never <code>null</code>.
     */
    public Set<String> getFailedEdgeIds();

}
