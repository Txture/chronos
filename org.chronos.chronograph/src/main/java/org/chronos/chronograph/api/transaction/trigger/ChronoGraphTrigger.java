package org.chronos.chronograph.api.transaction.trigger;

import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;

import java.io.Serializable;
import java.util.Set;

/**
 * A {@link ChronoGraphTrigger} is a handler that gets invoked by {@link ChronoGraph} during a {@link ChronoGraphTransaction#commit()} operation.
 *
 * <p>
 * This interface is intended to be implemented by clients. All implementations must be serializable/deserializable.
 * However, <b>do not implement it directly</b>. Instead, please implement at least one of:
 * <ul>
 * <li>{@link ChronoGraphPreCommitTrigger}</li>
 * <li>{@link ChronoGraphPrePersistTrigger}</li>
 * <li>{@link ChronoGraphPostPersistTrigger}</li>
 * <li>{@link ChronoGraphPostCommitTrigger}</li>
 * </ul>
 * </p>
 *
 * Implementing multiple trigger timing interfaces on the same class is supported as well.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphTrigger extends Serializable {

    /**
     * Returns the priority of this trigger.
     *
     * <p>
     * The higher the priority of a trigger, the earlier it is invoked among all triggers with the same timing. For example, a trigger with priority 100 will be fired <b>before</b> a trigger with priority 99, but <b>after</b> a trigger with priority 50.
     * </p>
     *
     * <p>
     * If two triggers have the same {@link #getPriority() priority}, then the execution order is unspecified.
     * </p>
     *
     * @return The priority of the trigger. Is assumed to remain unchanged over time.
     */
    public int getPriority();

}
