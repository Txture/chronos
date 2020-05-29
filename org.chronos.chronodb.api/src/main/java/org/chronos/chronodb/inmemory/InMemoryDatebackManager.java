package org.chronos.chronodb.inmemory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.impl.dateback.AbstractDatebackManager;
import org.chronos.chronodb.internal.api.dateback.log.DatebackOperation;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class InMemoryDatebackManager extends AbstractDatebackManager {

    private static final Comparator<LogKey> COMPARATOR = Comparator.comparing(LogKey::getBranch).thenComparing(LogKey::getTimestamp).thenComparing(LogKey::getId);

    private final TreeMap<LogKey, DatebackOperation> map = Maps.newTreeMap(COMPARATOR);


    public InMemoryDatebackManager(final ChronoDBInternal dbInstance) {
        super(dbInstance);
    }

    @Override
    protected void writeDatebackOperationToLog(final DatebackOperation operation) {
        checkNotNull(operation, "Precondition violation - argument 'operation' must not be NULL!");
        LogKey logKey = new LogKey(operation);
        this.map.put(logKey, operation);
    }

    @Override
    public List<DatebackOperation> getAllPerformedDatebackOperations() {
        return Lists.newArrayList(this.map.values());
    }

    @Override
    protected List<DatebackOperation> getDatebackOperationsPerformedOnBranchBetween(final String branch, final long dateTimeMin, final long dateTimeMax) {
        return Lists.newArrayList(this.map.subMap(new LogKey(branch, dateTimeMin, ""), true, new LogKey(branch, dateTimeMax + 1, ""), true).values());
    }

    @Override
    public void deleteLogsForBranch(final String name) {
        List<LogKey> keys = this.map.keySet().stream().filter(key -> Objects.equals(key.getBranch(), name)).collect(Collectors.toList());
        for(LogKey key : keys){
            this.map.remove(key);
        }
    }

    private static class LogKey {

        private final String branch;
        private final long timestamp;
        private final String id;

        public LogKey(String branch, long timestamp, String id) {
            this.branch = branch;
            this.timestamp = timestamp;
            this.id = id;
        }

        public LogKey(DatebackOperation operation) {
            this.branch = operation.getBranch();
            this.timestamp = operation.getWallClockTime();
            this.id = operation.getId();
        }

        public String getBranch() {
            return branch;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getId() {
            return this.id;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LogKey logKey = (LogKey) o;

            return id != null ? id.equals(logKey.id) : logKey.id == null;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }
}
