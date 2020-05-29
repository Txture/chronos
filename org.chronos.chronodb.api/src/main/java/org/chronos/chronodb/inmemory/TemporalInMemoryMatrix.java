package org.chronos.chronodb.inmemory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.api.key.TemporalKey;
import org.chronos.chronodb.internal.api.GetResult;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalDataMatrix;
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator;
import org.chronos.chronodb.internal.impl.temporal.InverseUnqualifiedTemporalKey;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;
import org.chronos.chronodb.internal.util.KeySetModifications;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.logging.ChronoLogger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public class TemporalInMemoryMatrix extends AbstractTemporalDataMatrix {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final NavigableMap<UnqualifiedTemporalKey, byte[]> contents;
    private final NavigableMap<InverseUnqualifiedTemporalKey, Boolean> inverseContents;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public TemporalInMemoryMatrix(final String keyspace, final long timestamp) {
        super(keyspace, timestamp);
        this.contents = new ConcurrentSkipListMap<>();
        this.inverseContents = new ConcurrentSkipListMap<>();
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public GetResult<byte[]> get(final long timestamp, final String key) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        QualifiedKey qKey = QualifiedKey.create(this.getKeyspace(), key);
        UnqualifiedTemporalKey temporalKey = UnqualifiedTemporalKey.create(key, timestamp);
        Entry<UnqualifiedTemporalKey, byte[]> floorEntry = this.contents.floorEntry(temporalKey);
        Entry<UnqualifiedTemporalKey, byte[]> ceilEntry = this.contents.higherEntry(temporalKey);
        if (floorEntry == null || floorEntry.getKey().getKey().equals(key) == false) {
            // we have no "next lower" bound -> we already know that the result will be empty.
            // now we need to check if we have an upper bound for the validity of our empty result...
            if (ceilEntry == null || ceilEntry.getKey().getKey().equals(key) == false) {
                // there is no value for this key (at all, not at any timestamp)
                return GetResult.createNoValueResult(qKey, Period.eternal());
            } else if (ceilEntry != null && ceilEntry.getKey().getKey().equals(key)) {
                // there is no value for this key, until a certain timestamp is reached
                Period period = Period.createRange(0, ceilEntry.getKey().getTimestamp());
                return GetResult.createNoValueResult(qKey, period);
            }
        } else {
            // we have a "next lower" bound -> we already know that the result will be non-empty.
            // now we need to check if we have an upper bound for the validity of our result...
            if (ceilEntry == null || ceilEntry.getKey().getKey().equals(key) == false) {
                // there is no further value for this key, therefore we have an open-ended period
                Period range = Period.createOpenEndedRange(floorEntry.getKey().getTimestamp());
                byte[] value = floorEntry.getValue();
                if (value != null && value.length <= 0) {
                    // value is non-null, but empty -> it's effectively null
                    value = null;
                }
                return GetResult.create(qKey, value, range);
            } else if (ceilEntry != null && ceilEntry.getKey().getKey().equals(key)) {
                // the value of the result is valid between the floor and ceiling entries
                Period period = Period.createRange(floorEntry.getKey().getTimestamp(),
                    ceilEntry.getKey().getTimestamp());
                byte[] value = floorEntry.getValue();
                if (value != null && value.length <= 0) {
                    // value is non-null, but empty -> it's effectively null
                    value = null;
                }
                return GetResult.create(qKey, value, period);
            }
        }
        // this code is effectively unreachable
        throw new RuntimeException("Unreachable code has been reached!");
    }

    @Override
    public void put(final long time, final Map<String, byte[]> contents) {
        checkArgument(time >= 0, "Precondition violation - argument 'time' must not be negative!");
        checkNotNull(contents, "Precondition violation - argument 'contents' must not be NULL!");
        if(contents.isEmpty()){
            return;
        }
        this.ensureCreationTimestampIsGreaterThanOrEqualTo(time);
        for (Entry<String, byte[]> entry : contents.entrySet()) {
            String key = entry.getKey();
            byte[] value = entry.getValue();
            UnqualifiedTemporalKey tk = UnqualifiedTemporalKey.create(key, time);
            if (value != null) {
                ChronoLogger.logTrace("[PUT] " + tk + "bytes[" + value.length + "]");
                this.contents.put(tk, value);
            } else {
                this.contents.put(tk, new byte[0]);
                ChronoLogger.logTrace("[PUT] " + tk + "NULL");
            }
            InverseUnqualifiedTemporalKey itk = InverseUnqualifiedTemporalKey.create(time, key);
            if (value != null) {
                this.inverseContents.put(itk, true);
            } else {
                this.inverseContents.put(itk, false);
            }
        }
    }

    @Override
    public KeySetModifications keySetModifications(final long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        // entry set is sorted in ascending order!
        Set<Entry<UnqualifiedTemporalKey, byte[]>> entrySet = this.contents.entrySet();
        Set<String> additions = Sets.newHashSet();
        Set<String> removals = Sets.newHashSet();
        // iterate over the full B-Tree key set (ascending order)
        Iterator<Entry<UnqualifiedTemporalKey, byte[]>> allEntriesIterator = entrySet.iterator();
        while (allEntriesIterator.hasNext()) {
            Entry<UnqualifiedTemporalKey, byte[]> currentEntry = allEntriesIterator.next();
            UnqualifiedTemporalKey currentKey = currentEntry.getKey();
            if (currentKey.getTimestamp() > timestamp) {
                continue;
            }
            String plainKey = currentKey.getKey();
            if (currentEntry.getValue() == null || currentEntry.getValue().length <= 0) {
                // removal
                additions.remove(plainKey);
                removals.add(plainKey);
            } else {
                // put
                additions.add(plainKey);
                removals.remove(plainKey);
            }
        }
        return new KeySetModifications(additions, removals);
    }

    @Override
    public Iterator<Long> history(final String key, final long lowerBound, final long upperBound, final Order order) {
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument(lowerBound >= 0, "Precondition violation - argument 'lowerBound' must not be negative!");
        checkArgument(upperBound >= 0, "Precondition violation - argument 'upperBound' must not be negative!");
        checkArgument(lowerBound <= upperBound, "Precondition violation - argument 'lowerBound' must be less than or equal to argument 'upperBound'!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        UnqualifiedTemporalKey tkMin = UnqualifiedTemporalKey.create(key, lowerBound);
        UnqualifiedTemporalKey tkMax = UnqualifiedTemporalKey.create(key, upperBound);
        NavigableMap<UnqualifiedTemporalKey, byte[]> subMap = this.contents.subMap(tkMin, true, tkMax, true);
        final Iterator<UnqualifiedTemporalKey> iterator;
        switch(order){
            case ASCENDING:
                iterator = subMap.keySet().iterator();
                break;
            case DESCENDING:
                iterator = subMap.descendingKeySet().iterator();
                break;
            default:
                throw new UnknownEnumLiteralException(order);
        }
        return new ChangeTimesIterator(iterator);
    }

    @Override
    public long lastCommitTimestamp(final String key, final long upperBound) {
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkArgument( upperBound >= 0, "Precondition violation - argument 'upperBound' must not be negative!");
        UnqualifiedTemporalKey floorKey = this.contents.floorKey(UnqualifiedTemporalKey.create(key, upperBound));
        if(floorKey == null){
            // has never been modified
            return -1;
        }else{
            return floorKey.getTimestamp();
        }
    }

    @Override
    public void insertEntries(final Set<UnqualifiedTemporalEntry> entries, final boolean force) {
        checkNotNull(entries, "Precondition violation - argument 'entries' must not be NULL!");
        if(entries.isEmpty()){
            return;
        }
        // update the creation timestamp in case that at least one of the entries is in the past
        long minTimestamp = entries.stream().mapToLong(entry -> entry.getKey().getTimestamp()).min().orElse(-1);
        if(minTimestamp > 0){
            this.ensureCreationTimestampIsGreaterThanOrEqualTo(minTimestamp);
        }
        // update primary index
        for (UnqualifiedTemporalEntry entry : entries) {
            UnqualifiedTemporalKey key = entry.getKey();
            byte[] value = entry.getValue();
            this.contents.put(key, value);
        }
        // update inverse index
        for (UnqualifiedTemporalEntry entry : entries) {
            String key = entry.getKey().getKey();
            long timestamp = entry.getKey().getTimestamp();
            InverseUnqualifiedTemporalKey itk = InverseUnqualifiedTemporalKey.create(timestamp, key);
            this.inverseContents.put(itk, entry.getValue() != null);
        }
    }

    @Override
    public CloseableIterator<UnqualifiedTemporalEntry> allEntriesIterator(final long minTimestamp, final long maxTimestamp) {
        return new AllEntriesIterator(this.contents.entrySet().iterator(), minTimestamp, maxTimestamp);
    }

    @Override
    public void rollback(final long timestamp) {
        Iterator<Entry<UnqualifiedTemporalKey, byte[]>> iterator = this.contents.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<UnqualifiedTemporalKey, byte[]> entry = iterator.next();
            if (entry.getKey().getTimestamp() > timestamp) {
                iterator.remove();
            }
        }
        Iterator<Entry<InverseUnqualifiedTemporalKey, Boolean>> iterator2 = this.inverseContents.entrySet().iterator();
        while (iterator2.hasNext()) {
            Entry<InverseUnqualifiedTemporalKey, Boolean> entry = iterator2.next();
            if (entry.getKey().getTimestamp() > timestamp) {
                iterator2.remove();
            }
        }
    }

    @Override
    public Iterator<TemporalKey> getModificationsBetween(final long timestampLowerBound,
                                                         final long timestampUpperBound) {
        checkArgument(timestampLowerBound >= 0,
            "Precondition violation - argument 'timestampLowerBound' must not be negative!");
        checkArgument(timestampUpperBound >= 0,
            "Precondition violation - argument 'timestampUpperBound' must not be negative!");
        checkArgument(timestampLowerBound <= timestampUpperBound,
            "Precondition violation - argument 'timestampLowerBound' must be less than or equal to 'timestampUpperBound'!");
        InverseUnqualifiedTemporalKey itkLow = InverseUnqualifiedTemporalKey.createMinInclusive(timestampLowerBound);
        InverseUnqualifiedTemporalKey itkHigh = InverseUnqualifiedTemporalKey.createMaxExclusive(timestampUpperBound);
        NavigableMap<InverseUnqualifiedTemporalKey, Boolean> subMap = this.inverseContents.subMap(itkLow, true, itkHigh,
            false);
        Iterator<InverseUnqualifiedTemporalKey> iterator = subMap.keySet().iterator();
        return Iterators.transform(iterator,
            itk -> TemporalKey.create(itk.getTimestamp(), this.getKeyspace(), itk.getKey()));
    }

    @Override
    public int purgeEntries(final Set<UnqualifiedTemporalKey> keys) {
        checkNotNull(keys, "Precondition violation - argument 'keys' must not be NULL!");
        int successfullyPurged = 0;
        for(UnqualifiedTemporalKey utk : keys){
            if (this.contents.containsKey(utk) == false) {
                continue;
            }
            this.contents.remove(utk);
            this.inverseContents.remove(utk.inverse());
            successfullyPurged++;
        }
        return successfullyPurged;
    }


    @Override
    public Set<UnqualifiedTemporalKey> purgeAllEntriesInTimeRange(final long purgeRangeStart, final long purgeRangeEnd) {
        NavigableMap<InverseUnqualifiedTemporalKey, Boolean> subMap = this.inverseContents.subMap(
            InverseUnqualifiedTemporalKey.create(purgeRangeStart, ""), true,
            InverseUnqualifiedTemporalKey.create(purgeRangeEnd + 1, ""), false
        );
        Set<UnqualifiedTemporalKey> inverseUnqualifiedTemporalKeys = subMap.keySet().stream()
            .filter(iutk -> iutk.getTimestamp() >= purgeRangeStart && iutk.getTimestamp() <= purgeRangeEnd)
            .map(InverseUnqualifiedTemporalKey::inverse)
            .collect(Collectors.toSet());
        inverseUnqualifiedTemporalKeys.forEach(utk -> {
            this.inverseContents.remove(utk.inverse());
            this.contents.remove(utk);
        });
        return inverseUnqualifiedTemporalKeys;
    }

    @Override
    public long size() {
        return this.contents.size();
    }

    @Override
    public void ensureCreationTimestampIsGreaterThanOrEqualTo(long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        if(this.getCreationTimestamp() > timestamp){
            this.setCreationTimestamp(timestamp);
        }
    }


    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private static class ChangeTimesIterator implements Iterator<Long> {

        private final Iterator<UnqualifiedTemporalKey> keyIterator;

        public ChangeTimesIterator(final Iterator<UnqualifiedTemporalKey> keyIterator) {
            this.keyIterator = keyIterator;
        }

        @Override
        public boolean hasNext() {
            return this.keyIterator.hasNext();
        }

        @Override
        public Long next() {
            return this.keyIterator.next().getTimestamp();
        }

    }

    private static class AllEntriesIterator extends AbstractCloseableIterator<UnqualifiedTemporalEntry> {

        private final Iterator<Entry<UnqualifiedTemporalKey, byte[]>> entryIterator;

        public AllEntriesIterator(final Iterator<Entry<UnqualifiedTemporalKey, byte[]>> entryIterator,
                                  final long minTimestamp, final long maxTimestamp) {
            this.entryIterator = Iterators.filter(entryIterator,
                entry ->{
                    long timestamp = entry.getKey().getTimestamp();
                    return timestamp >= minTimestamp && timestamp <= maxTimestamp;
                });
        }

        @Override
        protected boolean hasNextInternal() {
            return this.entryIterator.hasNext();
        }

        @Override
        public UnqualifiedTemporalEntry next() {
            Entry<UnqualifiedTemporalKey, byte[]> entry = this.entryIterator.next();
            UnqualifiedTemporalKey key = entry.getKey();
            byte[] value = entry.getValue();
            return new UnqualifiedTemporalEntry(key, value);
        }

        @Override
        protected void closeInternal() {
            // nothing to do for an in-memory matrix.
        }
    }

}
