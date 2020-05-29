package org.chronos.chronograph.internal.impl.structure.graph.readonly;

import org.chronos.chronograph.api.exceptions.TriggerAlreadyExistsException;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTriggerManager;
import org.chronos.chronograph.api.transaction.trigger.GraphTriggerMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.*;

public class ReadOnlyChronoGraphTriggerManager implements ChronoGraphTriggerManager {


    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ChronoGraphTriggerManager manager;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ReadOnlyChronoGraphTriggerManager(ChronoGraphTriggerManager manager) {
        checkNotNull(manager, "Precondition violation - argument 'manager' must not be NULL!");
        this.manager = manager;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public boolean existsTrigger(final String triggerName) {
        return this.manager.existsTrigger(triggerName);
    }

    @Override
    public boolean createTriggerIfNotPresent(final String triggerName, final Supplier<ChronoGraphTrigger> triggerSupplier) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean createTriggerIfNotPresent(final String triggerName, final Supplier<ChronoGraphTrigger> triggerSupplier, Object commitMetadata) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean createTriggerAndOverwrite(final String triggerName, final ChronoGraphTrigger trigger) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean createTriggerAndOverwrite(final String triggerName, final ChronoGraphTrigger trigger, Object commitMetadata) {
        return this.unsupportedOperation();
    }

    @Override
    public void createTrigger(final String triggerName, final ChronoGraphTrigger trigger) throws TriggerAlreadyExistsException {
        this.unsupportedOperation();
    }

    @Override
    public void createTrigger(final String triggerName, final ChronoGraphTrigger trigger, Object commitMetadata) throws TriggerAlreadyExistsException {
        this.unsupportedOperation();
    }

    @Override
    public boolean dropTrigger(final String triggerName) {
        return this.unsupportedOperation();
    }

    @Override
    public boolean dropTrigger(final String triggerName, Object commitMetadata) {
        return this.unsupportedOperation();
    }

    @Override
    public Set<String> getTriggerNames() {
        return Collections.unmodifiableSet(this.manager.getTriggerNames());
    }

    @Override
    public List<GraphTriggerMetadata> getTriggers() {
        return this.manager.getTriggers();
    }

    @Override
    public GraphTriggerMetadata getTrigger(final String triggerName) {
        return this.manager.getTrigger(triggerName);
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private <T> T unsupportedOperation() {
        throw new UnsupportedOperationException("This operation is not supported in read-only mode!");
    }
}
