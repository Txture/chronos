package org.chronos.chronograph.internal.impl.transaction.trigger;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronograph.api.exceptions.GraphTriggerClassNotFoundException;
import org.chronos.chronograph.api.exceptions.GraphTriggerException;
import org.chronos.chronograph.api.exceptions.TriggerAlreadyExistsException;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPostPersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPreCommitTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphPrePersistTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTrigger;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTriggerManager;
import org.chronos.chronograph.api.transaction.trigger.GraphTriggerMetadata;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.transaction.trigger.script.AbstractScriptedGraphTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphTriggerManagerImpl implements ChronoGraphTriggerManager, ChronoGraphTriggerManagerInternal {

    private static final Logger log = LoggerFactory.getLogger(ChronoGraphTriggerManagerImpl.class);

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ChronoGraphInternal graph;

    private List<Pair<String, ChronoGraphTrigger>> triggerCache;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ChronoGraphTriggerManagerImpl(ChronoGraphInternal graph) {
        checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
        this.graph = graph;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public synchronized boolean existsTrigger(final String triggerName) {
        this.validateTriggerName(triggerName);
        return this.getAllTriggers().stream().anyMatch(p -> p.getLeft().equals(triggerName));
    }

    @Override
    public synchronized boolean createTriggerIfNotPresent(final String triggerName, final Supplier<ChronoGraphTrigger> triggerSupplier) {
        return this.createTriggerIfNotPresent(triggerName, triggerSupplier, null);
    }

    @Override
    public synchronized boolean createTriggerIfNotPresent(final String triggerName, final Supplier<ChronoGraphTrigger> triggerSupplier, final Object commitMetadata) {
        this.validateTriggerName(triggerName);
        checkNotNull(triggerSupplier, "Precondition violation - argument 'triggerSupplier' must not be NULL!");
        if (this.existsTrigger(triggerName)) {
            // trigger already exists
            return false;
        }
        ChronoGraphTrigger trigger = triggerSupplier.get();
        if (trigger == null) {
            throw new IllegalArgumentException("The given trigger supplier has returned NULL!");
        }
        this.validateIncomingTrigger(trigger);
        // wrap the trigger inside a ChronoGraphTrigger to make sure we can always deserialize it properly
        ChronoGraphTriggerWrapper wrapper = new ChronoGraphTriggerWrapper(trigger);
        ChronoDBTransaction tx = this.backingTx();
        tx.put(ChronoGraphConstants.KEYSPACE_TRIGGERS, triggerName, wrapper);
        tx.commit(commitMetadata);
        this.clearTriggerCache();
        return true;
    }

    @Override
    public synchronized boolean createTriggerAndOverwrite(final String triggerName, final ChronoGraphTrigger trigger) {
        return this.createTriggerAndOverwrite(triggerName, trigger, null);
    }

    @Override
    public synchronized boolean createTriggerAndOverwrite(final String triggerName, final ChronoGraphTrigger trigger, Object commitMetadata) {
        this.validateTriggerName(triggerName);
        this.validateIncomingTrigger(trigger);
        // wrap the trigger inside a ChronoGraphTrigger to make sure we can always deserialize it properly
        ChronoGraphTriggerWrapper wrapper = new ChronoGraphTriggerWrapper(trigger);
        ChronoDBTransaction tx = this.backingTx();
        boolean result = tx.exists(ChronoGraphConstants.KEYSPACE_TRIGGERS, triggerName);
        tx.put(ChronoGraphConstants.KEYSPACE_TRIGGERS, triggerName, wrapper);
        tx.commit(commitMetadata);
        if (result) {
            this.clearTriggerCache();
        }
        return result;
    }

    @Override
    public synchronized void createTrigger(final String triggerName, final ChronoGraphTrigger trigger) throws TriggerAlreadyExistsException {
        this.createTrigger(triggerName, trigger, null);
    }

    @Override
    public synchronized void createTrigger(final String triggerName, final ChronoGraphTrigger trigger, final Object commitMetadata) throws TriggerAlreadyExistsException {
        boolean added = this.createTriggerIfNotPresent(triggerName, () -> trigger, commitMetadata);
        if (!added) {
            throw new TriggerAlreadyExistsException("A trigger with unique name '" + triggerName + "' already exists!");
        }
    }

    @Override
    public synchronized boolean dropTrigger(final String triggerName) {
        return dropTrigger(triggerName, null);
    }

    @Override
    public synchronized boolean dropTrigger(final String triggerName, final Object commitMetadata) {
        this.validateTriggerName(triggerName);
        ChronoDBTransaction tx = this.backingTx();
        boolean exists = tx.exists(ChronoGraphConstants.KEYSPACE_TRIGGERS, triggerName);
        if (!exists) {
            // nothing to do
            return false;
        }
        tx.remove(ChronoGraphConstants.KEYSPACE_TRIGGERS, triggerName);
        tx.commit(commitMetadata);
        this.clearTriggerCache();
        return true;
    }

    @Override
    public synchronized Set<String> getTriggerNames() {
        return this.getAllTriggers().stream()
            // only consider the names (left element of the pair)
            .map(Pair::getLeft).collect(Collectors.toSet());
    }

    @Override
    public List<GraphTriggerMetadata> getTriggers() {
        ChronoDBTransaction tx = this.backingTx();
        Set<String> allTriggerNames = tx.keySet(ChronoGraphConstants.KEYSPACE_TRIGGERS);
        return allTriggerNames.stream()
            .map(triggerName -> Pair.of(triggerName, (ChronoGraphTrigger)tx.get(ChronoGraphConstants.KEYSPACE_TRIGGERS, triggerName)))
            .filter(pair -> pair.getRight() != null)
            .sorted(NamedTriggerCategoryComparator.getInstance().thenComparing(NamedTriggerComparator.getInstance().reversed()))
            .map(pair -> this.getTriggerMetadata(pair.getLeft(), pair.getRight()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    @Override
    public GraphTriggerMetadata getTrigger(final String triggerName) {
        checkNotNull(triggerName, "Precondition violation - argument 'triggerName' must not be NULL!");
        ChronoDBTransaction tx = this.backingTx();
        ChronoGraphTrigger trigger = tx.get(ChronoGraphConstants.KEYSPACE_TRIGGERS, triggerName);
        return getTriggerMetadata(triggerName, trigger);
    }

    private GraphTriggerMetadata getTriggerMetadata(final String triggerName, final ChronoGraphTrigger persistedTrigger) {
        if (persistedTrigger == null) {
            return null;
        }
        ChronoGraphTrigger trigger = persistedTrigger;
        GraphTriggerException loadException = null;
        try {
            trigger = this.loadPersistedTrigger(triggerName, trigger);
        } catch (GraphTriggerException e) {
            loadException = e;
        }
        boolean isPreCommit = trigger instanceof ChronoGraphPreCommitTrigger;
        boolean isPrePersist = trigger instanceof ChronoGraphPrePersistTrigger;
        boolean isPostPersist = trigger instanceof ChronoGraphPostPersistTrigger;
        boolean isPostCommmit = trigger instanceof ChronoGraphPostCommitTrigger;
        String userScriptContent = null;
        if (trigger instanceof AbstractScriptedGraphTrigger) {
            AbstractScriptedGraphTrigger scriptedGraphTrigger = (AbstractScriptedGraphTrigger) trigger;
            userScriptContent = scriptedGraphTrigger.getUserScriptContent();
        }
        String triggerClassName = trigger.getClass().getName();
        return new GraphTriggerMetadataImpl(
            triggerName,
            triggerClassName,
            trigger.getPriority(),
            isPreCommit,
            isPrePersist,
            isPostPersist,
            isPostCommmit,
            userScriptContent,
            loadException
        );
    }

    // =================================================================================================================
    // INTERNAL METHODS
    // =================================================================================================================

    public synchronized List<Pair<String, ChronoGraphTrigger>> getAllTriggers() {
        if (this.triggerCache == null) {
            // load from DB
            List<Pair<String, ChronoGraphTrigger>> list = Lists.newArrayList();
            ChronoDBTransaction tx = this.backingTx();
            Set<String> triggerNames = tx.keySet(ChronoGraphConstants.KEYSPACE_TRIGGERS);
            for (String name : triggerNames) {
                try {
                    // load the trigger
                    ChronoGraphTrigger trigger = tx.get(ChronoGraphConstants.KEYSPACE_TRIGGERS, name);
                    trigger = this.loadPersistedTrigger(name, trigger);
                    list.add(Pair.of(name, trigger));
                } catch (Exception e) {
                    log.error("Failed to load Graph Trigger '" + name + "' from persistent store. This trigger will not be executed!", e);
                }
            }
            // sort them
            // note: comparators in java sort ascending by default, we want descending priority
            // order here, so we reverse the comparator.
            list.sort(NamedTriggerComparator.getInstance().reversed());
            this.triggerCache = Collections.unmodifiableList(list);
        }
        return this.triggerCache;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<Pair<String, ChronoGraphPreCommitTrigger>> getPreCommitTriggers() {
        return this.getAllTriggers().stream()
            .filter(pair -> pair.getRight() instanceof ChronoGraphPreCommitTrigger)
            .map(pair -> (Pair<String, ChronoGraphPreCommitTrigger>) (Pair) pair)
            .collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<Pair<String, ChronoGraphPrePersistTrigger>> getPrePersistTriggers() {
        return this.getAllTriggers().stream()
            .filter(pair -> pair.getRight() instanceof ChronoGraphPrePersistTrigger)
            .map(pair -> (Pair<String, ChronoGraphPrePersistTrigger>) (Pair) pair)
            .collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<Pair<String, ChronoGraphPostPersistTrigger>> getPostPersistTriggers() {
        return this.getAllTriggers().stream()
            .filter(pair -> pair.getRight() instanceof ChronoGraphPostPersistTrigger)
            .map(pair -> (Pair<String, ChronoGraphPostPersistTrigger>) (Pair) pair)
            .collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public List<Pair<String, ChronoGraphPostCommitTrigger>> getPostCommitTriggers() {
        return this.getAllTriggers().stream()
            .filter(pair -> pair.getRight() instanceof ChronoGraphPostCommitTrigger)
            .map(pair -> (Pair<String, ChronoGraphPostCommitTrigger>) (Pair) pair)
            .collect(Collectors.toList());
    }

    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private ChronoDBTransaction backingTx() {
        return this.graph.getBackingDB().tx(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
    }

    private void validateTriggerName(String triggerName) {
        checkNotNull(triggerName, "Precondition violation - argument 'triggerName' must not be NULL!");
        checkArgument(triggerName.isEmpty() == false, "Precondition violation - argumen 'triggerName' must not be the empty string!");
    }

    private void validateIncomingTrigger(ChronoGraphTrigger trigger) {
        if (trigger == null) {
            throw new NullPointerException("Precondition violation - the given trigger is NULL!");
        }
        //  make sure the trigger implements at least one of our sub-interfaces
        boolean atLeastOneInterfaceImplemented = false;
        if (trigger instanceof ChronoGraphPreCommitTrigger || trigger instanceof ChronoGraphPrePersistTrigger || trigger instanceof ChronoGraphPostPersistTrigger || trigger instanceof ChronoGraphPostCommitTrigger) {
            atLeastOneInterfaceImplemented = true;
        }
        if (!atLeastOneInterfaceImplemented) {
            throw new IllegalArgumentException(
                "The given trigger of type '" + trigger.getClass().getName() + "' does not implement any concrete trigger interface." +
                    " Please implement at least one of: "
                    + ChronoGraphPreCommitTrigger.class.getSimpleName() + ", "
                    + ChronoGraphPrePersistTrigger.class.getSimpleName() + ", "
                    + ChronoGraphPostPersistTrigger.class.getSimpleName() + " or "
                    + ChronoGraphPostCommitTrigger.class.getSimpleName()
                    + ".");
        }

        SerializationManager serializationManager = this.graph.getBackingDB().getSerializationManager();
        try {
            byte[] serialized = serializationManager.serialize(trigger);
            serializationManager.deserialize(serialized);
        } catch (Exception e) {
            throw new IllegalArgumentException("Precondition violation - the given trigger is not serializable! See root cause for details.", e);
        }
    }

    protected ChronoGraphTrigger loadPersistedTrigger(String name, ChronoGraphTrigger persistedTrigger) {
        ChronoGraphTrigger trigger = persistedTrigger;
        if (trigger instanceof ChronoGraphTriggerWrapper) {
            ChronoGraphTriggerWrapper wrapper = (ChronoGraphTriggerWrapper) trigger;
            Optional<ChronoGraphTrigger> wrappedTrigger = wrapper.getWrappedTrigger();
            if (!wrappedTrigger.isPresent()) {
                // failed to deserialize the wrapped trigger, print an error and ignore this trigger
                throw new GraphTriggerClassNotFoundException("Failed to load Graph Trigger '" + name + "' from persistent store! Is the required Java Class '" + wrapper.getTriggerClassName() + "' on the classpath? This trigger will not be executed!");
            } else {
                // wrapper initialized successfully, use the wrapped trigger
                trigger = wrappedTrigger.get();
            }
        }
        if (trigger instanceof AbstractScriptedGraphTrigger) {
            AbstractScriptedGraphTrigger scriptedTrigger = (AbstractScriptedGraphTrigger) trigger;
            // make sure that the trigger compiles and is instantiable
            scriptedTrigger.getCompiledScriptInstance();
        }
        return trigger;
    }

    private void clearTriggerCache() {
        this.triggerCache = null;
    }
}
