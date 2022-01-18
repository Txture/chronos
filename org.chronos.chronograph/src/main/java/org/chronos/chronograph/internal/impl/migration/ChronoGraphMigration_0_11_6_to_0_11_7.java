package org.chronos.chronograph.internal.impl.migration;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.chronos.chronodb.api.Dateback;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.key.ChronoIdentifier;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.impl.dump.meta.ChronoDBDumpMetadata;
import org.chronos.chronograph.api.branch.GraphBranch;
import org.chronos.chronograph.api.structure.record.IVertexRecord;
import org.chronos.chronograph.internal.ChronoGraphConstants;
import org.chronos.chronograph.internal.api.migration.ChronoGraphMigration;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.internal.impl.structure.record2.VertexRecord2;
import org.chronos.common.version.ChronosVersion;
import org.chronos.common.version.VersionKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation")
public class ChronoGraphMigration_0_11_6_to_0_11_7 implements ChronoGraphMigration {

    private static final Logger log = LoggerFactory.getLogger(ChronoGraphMigration_0_11_6_to_0_11_7.class);

    @Override
    public ChronosVersion getFromVersion() {
        return new ChronosVersion(0, 11, 6, VersionKind.RELEASE);
    }

    @Override
    public ChronosVersion getToVersion() {
        return new ChronosVersion(0, 11, 7, VersionKind.RELEASE);
    }

    @Override
    public void execute(final ChronoDBDumpMetadata dumpMetadata) {
        // nothing to do here
    }

    @Override
    public Object execute(final ChronoIdentifier chronoIdentifier, final Object value) {
        return ENTRY_UNCHANGED;
    }

    @Override
    public void execute(final ChronoGraphInternal graph) {
        log.info("Migrating ChronoGraph from " + graph.getStoredChronoGraphVersion().map(v -> v.toString()).orElse("<unknown>") + " to " + this.getToVersion() + ". This may take a while.");
        // get the branches, children first
        List<GraphBranch> branches = graph.getBranchManager().getBranches().stream()
            .sorted(Comparator.comparing(GraphBranch::getBranchingTimestamp).reversed())
            .collect(Collectors.toList());

        int branchIndex = 0;
        String logPrefix = "ChronoGraph Migration [" + this.getToVersion() + "]";
        // migrate the old triggers
        for (GraphBranch branch : branches) {
            String branchName = branch.getName();
            log.info(logPrefix + ": Starting migration of Branch '" + branchName + "' (" + branchIndex + " of " + branches.size() + ").");
            long branchingTimestamp = branch.getBranchingTimestamp();
            long branchNow = branch.getNow();
            List<Long> commitTimestamps = Lists.newArrayList(graph.getCommitTimestampsBetween(branchName, branchingTimestamp, branchNow, Order.DESCENDING, true));
            graph.getBackingDB().getDatebackManager().dateback(branchName, dateback -> {
                int commitIndex = 0;
                for (long commitTimestamp : commitTimestamps) {
                    log.info(logPrefix + " on Branch '" + branchName + "': migrating commit " + commitIndex + " of " + commitTimestamps.size() + " (" + new Date(commitTimestamp) + ")");
                    dateback.transformCommit(commitTimestamp, this::transformCommit);
                    commitIndex++;
                }
                log.info(logPrefix + ": Successfully migrated all " + commitTimestamps.size() + " commits on Branch '" + branchName + "', performing branch cleanup...");
            });
            branchIndex++;
        }
        log.info(logPrefix + " completed successfully on all " + branches.size() + " Branches");
    }

    public Map<QualifiedKey, Object> transformCommit(Map<QualifiedKey, Object> commitContents) {
        Map<QualifiedKey, Object> resultMap = Maps.newHashMap();
        for(Entry<QualifiedKey, Object> entry : commitContents.entrySet()){
            QualifiedKey qKey = entry.getKey();
            Object value = entry.getValue();
            Object newValue = this.transformEntry(qKey, value);
            resultMap.put(qKey, newValue);
        }
        return resultMap;
    }

    private Object transformEntry(QualifiedKey qKey, Object value){
        if(!ChronoGraphConstants.KEYSPACE_VERTEX.equals(qKey.getKeyspace())){
            // not in the vertex keyspace -> don't touch this.
            return Dateback.UNCHANGED;
        }
        if(value == null){
            // don't touch deletion markers
            return Dateback.UNCHANGED;
        }
        if(value instanceof VertexRecord2 == false){
            // not a vertex in the format we're looking for -> don't touch it
            return Dateback.UNCHANGED;
        }
        VertexRecord2 record = (VertexRecord2)value;
        // transform into the new format
        // (note: the builder always generates the latest format as output).
        return IVertexRecord.builder().fromRecord(record).build();
    }

}
