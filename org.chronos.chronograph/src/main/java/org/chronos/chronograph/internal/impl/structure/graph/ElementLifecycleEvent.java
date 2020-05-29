package org.chronos.chronograph.internal.impl.structure.graph;

public enum ElementLifecycleEvent {

    /**
     * The element was created for the first time. It is not yet part of the persistence.
     */
    CREATED,

    /**
     * Vertices only: an adjacent edge has been added or removed.
     */
    ADJACENT_EDGE_ADDED_OR_REMOVED,

    /**
     * A property on the element has changed.
     */
    PROPERTY_CHANGED,

    /**
     * The element has been deleted.
     */
    DELETED,

    /**
     * The element has been recreated from the OBSOLETE state.
     *
     * <p>
     * This occurs if the element was created, removed and is now being recreated (within the same transaction).
     * </p>
     */
    RECREATED_FROM_OBSOLETE,

    /**
     * The element has been recreated from the REMOVED state.
     *
     * <p>
     * This occurs if a persistent element is removed and is now being recreated (within the same transaction).
     * </p>
     */
    RECREATED_FROM_REMOVED,

    /**
     * The element has been saved to the persistence mechanism.
     */
    SAVED,

    /**
     * The element has been reloaded from the database, and no longer exists
     */
    RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT,

    /**
     * The element has been reloaded from the database and is now in sync
     */
    RELOADED_FROM_DB_AND_IN_SYNC

}
