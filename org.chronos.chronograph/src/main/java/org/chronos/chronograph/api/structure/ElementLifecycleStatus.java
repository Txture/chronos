package org.chronos.chronograph.api.structure;

import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleEvent;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public enum ElementLifecycleStatus {

    // =================================================================================================================
    // ENUM CONSTANTS
    // =================================================================================================================

    /**
     * The element was newly added in this transaction. It has never been persisted before.
     */
    NEW(ElementLifecycleStatus::stateTransitionFromNew),
    /**
     * The element is in-sync with the persistence backend and unchanged.
     */
    PERSISTED(ElementLifecycleStatus::stateTransitionFromPersisted),
    /**
     * The element was removed by the user in this transaction.
     */
    REMOVED(ElementLifecycleStatus::stateTransitionFromRemoved),
    /**
     * Properties of the element have changed. This includes also edge changes.
     */
    PROPERTY_CHANGED(ElementLifecycleStatus::stateTransitionFromPropertyChanged),
    /**
     * Edges of the vertex have changed. Properties are unchanged so far.
     */
    EDGE_CHANGED(ElementLifecycleStatus::stateTransitionFromEdgeChanged),
    /**
     * The element has been in state {@link #NEW}, but has been removed before ever being persisted.
     */
    OBSOLETE(ElementLifecycleStatus::stateTransitionFromObsolete);

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final Function<ElementLifecycleEvent, ElementLifecycleStatus> transitionFunction;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    ElementLifecycleStatus(Function<ElementLifecycleEvent, ElementLifecycleStatus> transitionFunction) {
        checkNotNull(transitionFunction, "Precondition violation - argument 'transitionFunction' must not be NULL!");
        this.transitionFunction = transitionFunction;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public boolean isDirty() {
        return this.equals(PERSISTED) == false;
    }

    // =================================================================================================================
    // STATE TRANSITION FUNCTIONS
    // =================================================================================================================

    private static ElementLifecycleStatus stateTransitionFromNew(ElementLifecycleEvent event) {
        ElementLifecycleStatus self = ElementLifecycleStatus.NEW;
        switch (event) {
            case CREATED:
                // no-op
                return self;
            case ADJACENT_EDGE_ADDED_OR_REMOVED:
                // ignore; NEW already includes EDGE_CHANGED
                return self;
            case DELETED:
                // new element was deleted -> obsolete
                return ElementLifecycleStatus.OBSOLETE;
            case SAVED:
                // new element was saved -> persisted
                return ElementLifecycleStatus.PERSISTED;
            case PROPERTY_CHANGED:
                // ignore; NEW already includes PROPERTY_CHANGED
                return self;
            case RECREATED_FROM_OBSOLETE:
                // the element has been recreated but it was obsolete before -> it's new now
                return self;
            case RECREATED_FROM_REMOVED:
                // the element has been recreated, but it was REMOVED before -> it is changed now
                return ElementLifecycleStatus.PROPERTY_CHANGED;
            case RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT:
                return ElementLifecycleStatus.REMOVED;
            case RELOADED_FROM_DB_AND_IN_SYNC:
                return ElementLifecycleStatus.PERSISTED;
            default:
                throw new UnknownEnumLiteralException(event);
        }
    }

    private static ElementLifecycleStatus stateTransitionFromPersisted(ElementLifecycleEvent event) {
        ElementLifecycleStatus self = ElementLifecycleStatus.PERSISTED;
        switch (event) {
            case CREATED:
                // we cannot "create" an element that already exists
                // and is in sync with persistence...
                throw new IllegalStateTransitionException(self, event);
            case ADJACENT_EDGE_ADDED_OR_REMOVED:
                // accept
                return ElementLifecycleStatus.EDGE_CHANGED;
            case PROPERTY_CHANGED:
                // accept
                return ElementLifecycleStatus.PROPERTY_CHANGED;
            case RECREATED_FROM_OBSOLETE:
                // we cannot be recreated, we still exist
                throw new IllegalStateTransitionException(self, event);
            case RECREATED_FROM_REMOVED:
                // we cannot be recreated, we still exist
                throw new IllegalStateTransitionException(self, event);
            case SAVED:
                // no-op
                return self;
            case DELETED:
                // accept
                return ElementLifecycleStatus.REMOVED;
            case RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT:
                return ElementLifecycleStatus.REMOVED;
            case RELOADED_FROM_DB_AND_IN_SYNC:
                return ElementLifecycleStatus.PERSISTED;
            default:
                throw new UnknownEnumLiteralException(event);
        }
    }

    private static ElementLifecycleStatus stateTransitionFromRemoved(ElementLifecycleEvent event) {
        ElementLifecycleStatus self = ElementLifecycleStatus.REMOVED;
        switch (event) {
            case CREATED:
                // we cannot be created when we are removed (would have to be RE-created)
                throw new IllegalStateTransitionException(self, event);
            case ADJACENT_EDGE_ADDED_OR_REMOVED:
                // on a removed element, no edges may be added and/or removed
                throw new IllegalStateTransitionException(self, event);
            case PROPERTY_CHANGED:
                // on a removed element, no property may be changed
                throw new IllegalStateTransitionException(self, event);
            case RECREATED_FROM_OBSOLETE:
                // cannot recreate an already existing element...
                throw new IllegalStateTransitionException(self, event);
            case RECREATED_FROM_REMOVED:
                // cannot recreate an already existing element...
                throw new IllegalStateTransitionException(self, event);
            case SAVED:
                // when we are removed, we cannot be saved
                throw new IllegalStateTransitionException(self, event);
            case DELETED:
                // well, we are already deleted...
                throw new IllegalStateTransitionException(self, event);
            case RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT:
                return ElementLifecycleStatus.REMOVED;
            case RELOADED_FROM_DB_AND_IN_SYNC:
                return ElementLifecycleStatus.PERSISTED;
            default:
                throw new UnknownEnumLiteralException(event);
        }
    }

    private static ElementLifecycleStatus stateTransitionFromPropertyChanged(ElementLifecycleEvent event) {
        ElementLifecycleStatus self = ElementLifecycleStatus.PROPERTY_CHANGED;
        switch (event) {
            case CREATED:
                // we cannot be created when we already exist
                throw new IllegalStateTransitionException(self, event);
            case ADJACENT_EDGE_ADDED_OR_REMOVED:
                // this state subsumes adjacent edge modification, so we stay in this state
                return self;
            case PROPERTY_CHANGED:
                // no-op
                return self;
            case RECREATED_FROM_OBSOLETE:
                // we cannot be recreated, we still exist
                throw new IllegalStateTransitionException(self, event);
            case RECREATED_FROM_REMOVED:
                // we cannot be recreated, we still exist
                throw new IllegalStateTransitionException(self, event);
            case SAVED:
                // this element has been saved and is clean now
                return ElementLifecycleStatus.PERSISTED;
            case DELETED:
                // the element has been deleted
                return ElementLifecycleStatus.REMOVED;
            case RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT:
                return ElementLifecycleStatus.REMOVED;
            case RELOADED_FROM_DB_AND_IN_SYNC:
                return ElementLifecycleStatus.PERSISTED;
            default:
                throw new UnknownEnumLiteralException(event);
        }
    }

    private static ElementLifecycleStatus stateTransitionFromEdgeChanged(ElementLifecycleEvent event) {
        ElementLifecycleStatus self = ElementLifecycleStatus.EDGE_CHANGED;
        switch (event) {
            case CREATED:
                // an element that already exists cannot be recreated...
                throw new IllegalStateTransitionException(self, event);
            case ADJACENT_EDGE_ADDED_OR_REMOVED:
                // no-op
                return self;
            case PROPERTY_CHANGED:
                // property change subsumes edge change, so we accept this change
                return ElementLifecycleStatus.PROPERTY_CHANGED;
            case SAVED:
                // accept
                return ElementLifecycleStatus.PERSISTED;
            case DELETED:
                // accept
                return ElementLifecycleStatus.REMOVED;
            case RECREATED_FROM_OBSOLETE:
                // cannot recreate an already existing element...
                throw new IllegalStateTransitionException(self, event);
            case RECREATED_FROM_REMOVED:
                // cannot recreate an already existing element...
                throw new IllegalStateTransitionException(self, event);
            case RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT:
                return ElementLifecycleStatus.REMOVED;
            case RELOADED_FROM_DB_AND_IN_SYNC:
                return ElementLifecycleStatus.PERSISTED;
            default:
                throw new UnknownEnumLiteralException(event);
        }
    }

    private static ElementLifecycleStatus stateTransitionFromObsolete(ElementLifecycleEvent event) {
        ElementLifecycleStatus self = ElementLifecycleStatus.OBSOLETE;
        switch (event) {
            case CREATED:
                // an element that is obsolete cannot be created (only RE-created)
                throw new IllegalStateTransitionException(self, event);
            case ADJACENT_EDGE_ADDED_OR_REMOVED:
                // an element that is obsolete cannot receive any changes
                throw new IllegalStateTransitionException(self, event);
            case PROPERTY_CHANGED:
                // an element that is obsolete cannot receive any changes
                throw new IllegalStateTransitionException(self, event);
            case SAVED:
                // an obsolete element cannot be saved
                throw new IllegalStateTransitionException(self, event);
            case DELETED:
                // an obsolete element has already been deleted
                throw new IllegalStateTransitionException(self, event);
            case RECREATED_FROM_OBSOLETE:
                // this never occurs on an existing element; recreations
                // always occur on NEW elements.
                throw new IllegalStateTransitionException(self, event);
            case RECREATED_FROM_REMOVED:
                // this never occurs on an existing element; recreations
                // always occur on NEW elements.
                throw new IllegalStateTransitionException(self, event);
            case RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT:
                return ElementLifecycleStatus.REMOVED;
            case RELOADED_FROM_DB_AND_IN_SYNC:
                return ElementLifecycleStatus.PERSISTED;
            default:
                throw new UnknownEnumLiteralException(event);
        }
    }


    public ElementLifecycleStatus nextState(ElementLifecycleEvent event) {
        checkNotNull(event, "Precondition violation - argument 'event' must not be NULL!");
        return this.transitionFunction.apply(event);
    }

    public static class IllegalStateTransitionException extends RuntimeException {

        private final ElementLifecycleStatus status;
        private final ElementLifecycleEvent event;

        public IllegalStateTransitionException(ElementLifecycleStatus status, ElementLifecycleEvent event) {
            this.status = status;
            this.event = event;
        }

        @Override
        public String getMessage() {
            return "There is no transition from state '" + this.status + "' with event '" + this.event + "'!";
        }
    }

}
