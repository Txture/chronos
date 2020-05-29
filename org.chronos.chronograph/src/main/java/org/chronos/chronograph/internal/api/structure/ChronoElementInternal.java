package org.chronos.chronograph.internal.api.structure;

import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;

public interface ChronoElementInternal extends ChronoElement {

    public void notifyPropertyChanged(ChronoProperty<?> chronoProperty);

    /**
     * Validates the graph invariant on this element.
     * <p>
     * The graph invariant states the following conditions:
     * <ul>
     * If a vertex has an IN edge, then that edge must reference the vertex as in-vertex
     * If a vertex has an OUT edge, then that edge must reference the vertex as out-vertex
     * For any edge, the IN vertex must reference the edge as incoming, and the OUT vertex must reference the edge as outgoing
     * If a vertex is being deleted, all adjacent edges must be deleted.
     * If an edge is being deleted, it must no longer appear as adjacent edge in the two neighboring vertices.
     * </ul>
     * <p>
     * If any of these conditions are violated,
     */
    public void validateGraphInvariant();

}
