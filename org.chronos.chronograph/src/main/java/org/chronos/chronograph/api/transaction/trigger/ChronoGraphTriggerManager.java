package org.chronos.chronograph.api.transaction.trigger;

import org.chronos.chronograph.api.exceptions.TriggerAlreadyExistsException;
import org.chronos.chronograph.api.structure.ChronoGraph;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The {@link ChronoGraphTriggerManager} manages the {@link ChronoGraphTrigger}s in a {@link ChronoGraph} instance.
 *
 * <p>
 * An instance of this class can be retrieved via {@link ChronoGraph#getTriggerManager()}.
 * </p>
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphTriggerManager {

    /**
     * Checks if the trigger with the given name exists.
     *
     * @param triggerName The unique name of the trigger to check. Must not be <code>null</code> or empty.
     * @return <code>true</code> if the trigger exists, otherwise <code>false</code>.
     */
    public boolean existsTrigger(String triggerName);

    /**
     * Creates a trigger if there is no trigger registered to the given name.
     *
     * <p>
     * If there already exists a trigger with the given name, this method does nothing and returns immmediately.
     * </p>
     *
     * @param triggerName     The unique name of the trigger. Must not be <code>null</code> or empty.
     * @param triggerSupplier The supplier for the trigger. Will be invoked if and only if there is no trigger with the given name yet. Will be invoked at most once. Must not be <code>null</code>. The {@link Supplier#get()} method must not return <code>null</code>.
     * @return <code>true</code> code if the trigger was added successfully, or <code>false</code> if there already was a trigger registered to the given name.
     * @see #createTrigger(String, ChronoGraphTrigger)
     * @see #createTriggerAndOverwrite(String, ChronoGraphTrigger)
     */
    public boolean createTriggerIfNotPresent(String triggerName, Supplier<ChronoGraphTrigger> triggerSupplier);

    /**
     * Creates a trigger if there is no trigger registered to the given name.
     *
     * <p>
     * If there already exists a trigger with the given name, this method does nothing and returns immmediately.
     * </p>
     *
     * @param triggerName     The unique name of the trigger. Must not be <code>null</code> or empty.
     * @param triggerSupplier The supplier for the trigger. Will be invoked if and only if there is no trigger with the given name yet. Will be invoked at most once. Must not be <code>null</code>. The {@link Supplier#get()} method must not return <code>null</code>.
     * @param commitMetadata The metadata for the commit of creating a new trigger. May be <code>null</code>.
     * @return <code>true</code> code if the trigger was added successfully, or <code>false</code> if there already was a trigger registered to the given name.
     * @see #createTrigger(String, ChronoGraphTrigger)
     * @see #createTriggerAndOverwrite(String, ChronoGraphTrigger)
     */
    public boolean createTriggerIfNotPresent(String triggerName, Supplier<ChronoGraphTrigger> triggerSupplier, Object commitMetadata);

    /**
     * Creates a trigger with the given unique name, overriding any previous trigger.
     *
     * @param triggerName The unique name of the trigger. Must not be <code>null</code> or empty.
     * @param trigger     The trigger to create. Must not be <code>null</code>.
     * @return <code>true</code> if an existing trigger has been overwritten, or <code>false</code> if no trigger existed previously for the given name.
     * @see #createTrigger(String, ChronoGraphTrigger)
     * @see #createTriggerIfNotPresent(String, Supplier)
     */
    public boolean createTriggerAndOverwrite(String triggerName, ChronoGraphTrigger trigger);

    /**
     * Creates a trigger with the given unique name, overriding any previous trigger.
     *
     * @param triggerName The unique name of the trigger. Must not be <code>null</code> or empty.
     * @param trigger     The trigger to create. Must not be <code>null</code>.
     * @param commitMetadata The metadata for the commit of creating a new trigger. May be <code>null</code>.
     * @return <code>true</code> if an existing trigger has been overwritten, or <code>false</code> if no trigger existed previously for the given name.
     * @see #createTrigger(String, ChronoGraphTrigger)
     * @see #createTriggerIfNotPresent(String, Supplier)
     */
    public boolean createTriggerAndOverwrite(String triggerName, ChronoGraphTrigger trigger, Object commitMetadata);

    /**
     * Creates a trigger with the given unique name.
     *
     * <p>
     * If a trigger is already bound to the given name, a {@link TriggerAlreadyExistsException} will be thrown.
     * </p>
     *
     * @param triggerName The unique name of the trigger. Must not be <code>null</code> or empty.
     * @param trigger     The trigger to create. Must not be <code>null</code>.
     * @throws TriggerAlreadyExistsException if there already exists a trigger with the given unique name.
     */
    public void createTrigger(String triggerName, ChronoGraphTrigger trigger) throws TriggerAlreadyExistsException;

    /**
     * Creates a trigger with the given unique name.
     *
     * <p>
     * If a trigger is already bound to the given name, a {@link TriggerAlreadyExistsException} will be thrown.
     * </p>
     *
     * @param triggerName The unique name of the trigger. Must not be <code>null</code> or empty.
     * @param trigger     The trigger to create. Must not be <code>null</code>.
     * @param commitMetadata The metadata for the commit of creating a new trigger. May be <code>null</code>.
     * @throws TriggerAlreadyExistsException if there already exists a trigger with the given unique name.
     */
    public void createTrigger(String triggerName, ChronoGraphTrigger trigger, Object commitMetadata) throws TriggerAlreadyExistsException;

    /**
     * Unconditionally drops the trigger with the given unique name.
     *
     * @param triggerName The name of the trigger to drop. Must not be <code>null</code> or empty.
     * @return <code>true</code> if a trigger with the given name was dropped, or <code>false</code> if there was no such trigger.
     */
    public boolean dropTrigger(String triggerName);

    /**
     * Unconditionally drops the trigger with the given unique name.
     *
     * @param triggerName The name of the trigger to drop. Must not be <code>null</code> or empty.
     * @param commitMetadata The metadata for the commit of dropping a trigger. May be <code>null</code>.
     * @return <code>true</code> if a trigger with the given name was dropped, or <code>false</code> if there was no such trigger.
     */
    public boolean dropTrigger(String triggerName, Object commitMetadata);

    /**
     * Returns the unique names of all known triggers.
     *
     * @return The unique names of all known triggers. May be empty, but never <code>null</code>. Returns an immutable view.
     */
    public Set<String> getTriggerNames();

    /**
     * Returns the {@link GraphTriggerMetadata metadata} of all known triggers, including invalid ones.
     *
     * The resulting list will be ordered in the actual trigger execution order.
     *
     * @return
     */
    public List<GraphTriggerMetadata> getTriggers();

    public GraphTriggerMetadata getTrigger(final String triggerName);


}
