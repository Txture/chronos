package org.chronos.chronograph.internal.impl.migration;

import org.chronos.chronodb.api.Dateback;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.transaction.trigger.ChronoGraphTrigger;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.migration.ChronoGraphMigration;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.transaction.trigger.ChronoGraphTriggerWrapper;
import org.chronos.common.version.ChronosVersion;
import org.chronos.common.version.VersionKind;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ChronoGraphMigration_0_11_1_to_0_11_2 implements ChronoGraphMigration {

    @Override
    public ChronosVersion getFromVersion() {
        return new ChronosVersion(0, 11, 1, VersionKind.RELEASE);
    }

    @Override
    public ChronosVersion getToVersion() {
        return new ChronosVersion(0, 11, 2, VersionKind.RELEASE);
    }

    @Override
    public void execute(final ChronoGraphInternal graph) {
        // get the branches, children first
        List<GraphBranch> branches = graph.getBranchManager().getBranches().stream()
            .sorted(Comparator.comparing(GraphBranch::getBranchingTimestamp).reversed())
            .collect(Collectors.toList());

        // migrate the old triggers
        for(GraphBranch branch : branches){
            String branchName = branch.getName();
            graph.getBackingDB().getDatebackManager().dateback(branchName, dateback -> {
                dateback.transformValuesOfKeyspace(ChronoGraphConstants.KEYSPACE_TRIGGERS, this::transformTrigger);
            });
        }
    }

    @Override
    public void execute(final ChronoDBDumpMetadata dumpMetadata) {
        // nothing to do here
    }

    @Override
    public Object execute(final ChronoIdentifier chronoIdentifier, final Object value) {
        if(ChronoGraphConstants.KEYSPACE_TRIGGERS.equals(chronoIdentifier.getKeyspace())){
            if(value instanceof ChronoGraphTrigger){
                // transform the trigger entry
                return this.transformTrigger((ChronoGraphTrigger)value);
            }
        }
        return ChronoGraphMigration.ENTRY_UNCHANGED;
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private Object transformTrigger(String key, long timestamp, Object oldValue) {
        // the old value is an implementation of a ChronoGraph trigger interface.
        if(oldValue instanceof ChronoGraphTrigger ){
            return transformTrigger((ChronoGraphTrigger)oldValue);
        }else{
            // whatever it is, don't touch it
            return Dateback.UNCHANGED;
        }
    }

    private Object transformTrigger(ChronoGraphTrigger trigger){
        if(trigger instanceof ChronoGraphTriggerWrapper){
            // this migration has already taken place, nothing to do
            return trigger;
        }
        // wrap the trigger into a wrapper object for safe deserialization handling if the class is absent.
        return new ChronoGraphTriggerWrapper(trigger);
    }
}
