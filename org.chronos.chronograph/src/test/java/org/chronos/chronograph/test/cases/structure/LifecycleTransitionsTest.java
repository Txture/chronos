package org.chronos.chronograph.test.cases.structure;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.chronos.chronograph.internal.impl.structure.graph.ElementLifecycleEvent;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus.IllegalStateTransitionException;
import org.chronos.chronograph.test.base.ChronoGraphUnitTest;
import org.chronos.common.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map.Entry;

import static com.google.common.base.Preconditions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(UnitTest.class)
public class LifecycleTransitionsTest extends ChronoGraphUnitTest {


    @Test
    public void lifecycleStateTransitionsProduceTheExpectedResults() {
        // we use variables here to have shorter names (and not having to re-type the enum name in all the places)
        ElementLifecycleStatus sNew = ElementLifecycleStatus.NEW;
        ElementLifecycleStatus sPersisted = ElementLifecycleStatus.PERSISTED;
        ElementLifecycleStatus sObsolete = ElementLifecycleStatus.OBSOLETE;
        ElementLifecycleStatus sRemoved = ElementLifecycleStatus.REMOVED;
        ElementLifecycleStatus sPropertyChanged = ElementLifecycleStatus.PROPERTY_CHANGED;
        ElementLifecycleStatus sEdgeChanged = ElementLifecycleStatus.EDGE_CHANGED;

        // we use variables here to have shorter names (and not having to re-type the enum name in all the places)
        ElementLifecycleEvent eCreated = ElementLifecycleEvent.CREATED;
        ElementLifecycleEvent eRecreatedFromRemoved = ElementLifecycleEvent.RECREATED_FROM_REMOVED;
        ElementLifecycleEvent eRecreatedFromObsolete = ElementLifecycleEvent.RECREATED_FROM_OBSOLETE;
        ElementLifecycleEvent eSaved = ElementLifecycleEvent.SAVED;
        ElementLifecycleEvent eDeleted = ElementLifecycleEvent.DELETED;
        ElementLifecycleEvent ePropertyChanged = ElementLifecycleEvent.PROPERTY_CHANGED;
        ElementLifecycleEvent eEdgeChanged = ElementLifecycleEvent.ADJACENT_EDGE_ADDED_OR_REMOVED;
        ElementLifecycleEvent eReloadedInSync = ElementLifecycleEvent.RELOADED_FROM_DB_AND_IN_SYNC;
        ElementLifecycleEvent eReloadedGone = ElementLifecycleEvent.RELOADED_FROM_DB_AND_NO_LONGER_EXISTENT;

        TransitionChecker checker = new TransitionChecker();
        Object invalid = TransitionChecker.INVALID;

        // transitions in state NEW
        checker.checkTransition(sNew, eCreated, sNew);
        checker.checkTransition(sNew, eDeleted, sObsolete);
        checker.checkTransition(sNew, eSaved, sPersisted);
        checker.checkTransition(sNew, eEdgeChanged, sNew);
        checker.checkTransition(sNew, ePropertyChanged, sNew);
        checker.checkTransition(sNew, eRecreatedFromObsolete, sNew);
        checker.checkTransition(sNew, eRecreatedFromRemoved, sPropertyChanged);


        // transitions in state OBSOLETE
        checker.checkTransition(sObsolete, eCreated, invalid);
        checker.checkTransition(sObsolete, eDeleted, invalid);
        checker.checkTransition(sObsolete, eSaved, invalid);
        checker.checkTransition(sObsolete, eEdgeChanged, invalid);
        checker.checkTransition(sObsolete, ePropertyChanged, invalid);
        checker.checkTransition(sObsolete, eRecreatedFromObsolete, invalid);
        checker.checkTransition(sObsolete, eRecreatedFromRemoved, invalid);


        // transitions in state PERSISTED
        checker.checkTransition(sPersisted, eCreated, invalid);
        checker.checkTransition(sPersisted, eDeleted, sRemoved);
        checker.checkTransition(sPersisted, eSaved, sPersisted);
        checker.checkTransition(sPersisted, eEdgeChanged, sEdgeChanged);
        checker.checkTransition(sPersisted, ePropertyChanged, sPropertyChanged);
        checker.checkTransition(sPersisted, eRecreatedFromObsolete, invalid);
        checker.checkTransition(sPersisted, eRecreatedFromRemoved, invalid);

        // transitions in state EDGE_CHANGED
        checker.checkTransition(sEdgeChanged, eCreated, invalid);
        checker.checkTransition(sEdgeChanged, eDeleted, sRemoved);
        checker.checkTransition(sEdgeChanged, eSaved, sPersisted);
        checker.checkTransition(sEdgeChanged, eEdgeChanged, sEdgeChanged);
        checker.checkTransition(sEdgeChanged, ePropertyChanged, sPropertyChanged);
        checker.checkTransition(sEdgeChanged, eRecreatedFromObsolete, invalid);
        checker.checkTransition(sEdgeChanged, eRecreatedFromRemoved, invalid);

        // transitions in state PROPERTY_CHANGED
        checker.checkTransition(sPropertyChanged, eCreated, invalid);
        checker.checkTransition(sPropertyChanged, eDeleted, sRemoved);
        checker.checkTransition(sPropertyChanged, eSaved, sPersisted);
        checker.checkTransition(sPropertyChanged, eEdgeChanged, sPropertyChanged);
        checker.checkTransition(sPropertyChanged, ePropertyChanged, sPropertyChanged);
        checker.checkTransition(sPropertyChanged, eRecreatedFromObsolete, invalid);
        checker.checkTransition(sPropertyChanged, eRecreatedFromRemoved, invalid);

        // transitions in state REMOVED
        checker.checkTransition(sRemoved, eCreated, invalid);
        checker.checkTransition(sRemoved, eDeleted, invalid);
        checker.checkTransition(sRemoved, eSaved, invalid);
        checker.checkTransition(sRemoved, eEdgeChanged, invalid);
        checker.checkTransition(sRemoved, ePropertyChanged, invalid);
        checker.checkTransition(sRemoved, eRecreatedFromObsolete, invalid);
        checker.checkTransition(sRemoved, eRecreatedFromRemoved, invalid);

        // from any state, we should be able to run the reloaded events
        for (ElementLifecycleStatus status : ElementLifecycleStatus.values()) {
            checker.checkTransition(status, eReloadedInSync, sPersisted);
            checker.checkTransition(status, eReloadedGone, sRemoved);
        }

        checker.assertAllTransitionsWereChecked();

    }


    private static class TransitionChecker {

        public static final Object INVALID = new Object();

        private final SetMultimap<ElementLifecycleStatus, ElementLifecycleEvent> testedTransitions = HashMultimap.create();

        public void checkTransition(ElementLifecycleStatus status, ElementLifecycleEvent event, Object result) {
            checkNotNull(status, "Precondition violation - argument 'status' must not be NULL!");
            checkNotNull(event, "Precondition violation - argument 'event' must not be NULL!");
            checkNotNull(result, "Precondition violation - argument 'result' must not be NULL!");
            if (result.equals(INVALID)) {
                this.assertTransitionInvalid(status, event);
            } else {
                this.checkTransition(status, event, (ElementLifecycleStatus) result);
            }
            this.testedTransitions.put(status, event);
        }

        public void checkTransition(ElementLifecycleStatus oldStatus, ElementLifecycleEvent event, ElementLifecycleStatus newStatus) {
            checkNotNull(oldStatus, "Precondition violation - argument 'oldStatus' must not be NULL!");
            checkNotNull(event, "Precondition violation - argument 'event' must not be NULL!");
            checkNotNull(newStatus, "Precondition violation - argument 'newStatus' must not be NULL!");
            assertThat(oldStatus.nextState(event), is(newStatus));
            this.testedTransitions.put(oldStatus, event);
        }

        public void assertTransitionInvalid(ElementLifecycleStatus status, ElementLifecycleEvent event) {
            checkNotNull(status, "Precondition violation - argument 'status' must not be NULL!");
            checkNotNull(event, "Precondition violation - argument 'event' must not be NULL!");
            try {
                ElementLifecycleStatus newStatus = status.nextState(event);
                fail("Managed to transition from state " + status + " to " + newStatus + " with event " + event + " (expected transition to be invalid).");
            } catch (IllegalStateTransitionException expected) {
                // pass
                this.testedTransitions.put(status, event);
            }
        }


        public void assertAllTransitionsWereChecked() {
            SetMultimap<ElementLifecycleStatus, ElementLifecycleEvent> untestedTransitions = HashMultimap.create();
            for (ElementLifecycleStatus status : ElementLifecycleStatus.values()) {
                for (ElementLifecycleEvent event : ElementLifecycleEvent.values()) {
                    if (!this.testedTransitions.containsEntry(status, event)) {
                        untestedTransitions.put(status, event);
                    }
                }
            }
            if (!untestedTransitions.isEmpty()) {
                // there were untested transitions, create an error message
                StringBuilder msg = new StringBuilder();
                msg.append("There were ");
                msg.append(untestedTransitions.size());
                msg.append(" untested lifecycle state transitions. Please add them to the test. ");
                msg.append("The untested transitions are:\n");
                for (Entry<ElementLifecycleStatus, ElementLifecycleEvent> entry : untestedTransitions.entries()) {
                    ElementLifecycleStatus status = entry.getKey();
                    ElementLifecycleEvent event = entry.getValue();
                    msg.append("\t");
                    msg.append(status);
                    msg.append("->");
                    msg.append(event);
                    msg.append("\n");
                }
                fail(msg.toString());
            }
        }
    }


}
