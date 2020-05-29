package org.chronos.chronodb.inmemory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.internal.impl.engines.base.AbstractCommitMetadataStore;
import org.chronos.chronodb.internal.impl.engines.base.ChronosInternalCommitMetadata;
import org.chronos.chronodb.internal.util.NavigableMapUtils;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.base.Preconditions.*;

public class InMemoryCommitMetadataStore extends AbstractCommitMetadataStore {

    private NavigableMap<Long, byte[]> commitMetadataMap;

    public InMemoryCommitMetadataStore(final ChronoDB owningDB, final Branch owningBranch) {
        super(owningDB, owningBranch);
        this.commitMetadataMap = new ConcurrentSkipListMap<>();
    }

    @Override
    protected byte[] getInternal(final long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        return this.commitMetadataMap.get(timestamp);
    }

    @Override
    protected void putInternal(final long commitTimestamp, final byte[] serializedMetadata) {
        checkArgument(commitTimestamp >= 0,
            "Precondition violation - argument 'commitTimestamp' must not be negative!");
        checkNotNull(serializedMetadata, "Precondition violation - argument 'serializedMetadata' must not be NULL!");
        this.commitMetadataMap.put(commitTimestamp, serializedMetadata);
    }

    @Override
    protected void rollbackToTimestampInternal(final long timestamp) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(timestamp, false, Long.MAX_VALUE, true);
        subMap.clear();
    }

    @Override
    public boolean purge(final long commitTimestamp) {
        if (this.commitMetadataMap.containsKey(commitTimestamp) == false) {
            return false;
        }
        this.commitMetadataMap.remove(commitTimestamp);
        return true;
    }

    @Override
    public Iterator<Long> getCommitTimestampsBetween(final long from, final long to, final Order order, boolean includeSystemInternalCommits) {
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        // fail-fast if the period is empty
        if (from > to) {
            return Collections.emptyIterator();
        }
        NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(from, true, to, true);
        if (!includeSystemInternalCommits) {
            // apply filter
            subMap = Maps.filterValues(subMap, this::isBinaryUserCommitMetadata);
        }
        switch (order) {
            case ASCENDING:
                // note: NavigableMap#keySet() and #entrySet() are sorted in ascending order by default.
                return Iterators.unmodifiableIterator(subMap.keySet().iterator());
            case DESCENDING:
                return Iterators.unmodifiableIterator(subMap.descendingKeySet().iterator());
            default:
                throw new UnknownEnumLiteralException(order);
        }
    }

    @Override
    public Iterator<Entry<Long, Object>> getCommitMetadataBetween(final long from, final long to, final Order order, boolean includeSystemInternalCommits) {
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        // fail-fast if the period is empty
        if (from > to) {
            return Collections.emptyIterator();
        }
        NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(from, true, to, true);
        final Iterator<Entry<Long, byte[]>> rawIterator;
        switch (order) {
            case ASCENDING:
                rawIterator = subMap.entrySet().iterator();
                break;
            case DESCENDING:
                rawIterator = subMap.descendingMap().entrySet().iterator();
                break;
            default:
                throw new UnknownEnumLiteralException(order);
        }
        Iterator<Entry<Long, Object>> iterator = Iterators.transform(rawIterator, this::mapSerialEntryToPair);
        if (!includeSystemInternalCommits) {
            iterator = Iterators.filter(iterator, this::isUserCommit);
        }
        return Iterators.unmodifiableIterator(iterator);
    }

    @Override
    public Iterator<Long> getCommitTimestampsPaged(final long minTimestamp, final long maxTimestamp, final int pageSize,
                                                   final int pageIndex, final Order order, final boolean includeSystemInternalCommits) {
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
        checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        // fail-fast if the period is empty
        if (minTimestamp > maxTimestamp) {
            return Collections.emptyIterator();
        }
        int elementsToSkip = pageSize * pageIndex;
        NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(minTimestamp, true, maxTimestamp, true);
        if (!includeSystemInternalCommits) {
            // apply filter
            subMap = Maps.filterValues(subMap, this::isBinaryUserCommitMetadata);
        }
        final Iterator<Long> rawIterator;
        switch (order) {
            case ASCENDING:
                rawIterator = subMap.keySet().iterator();
                break;
            case DESCENDING:
                rawIterator = subMap.descendingKeySet().iterator();
                break;
            default:
                throw new UnknownEnumLiteralException(order);
        }
        // skip entries of the iterator to arrive at the correct page
        for (int i = 0; i < elementsToSkip && rawIterator.hasNext(); i++) {
            rawIterator.next();
        }
        // limit the rest of the iterator to the given page size
        return Iterators.unmodifiableIterator(Iterators.limit(rawIterator, pageSize));
    }

    @Override
    public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final long minTimestamp, final long maxTimestamp,
                                                                final int pageSize, final int pageIndex, final Order order,
                                                                final boolean includeSystemInternalCommits) {
        checkArgument(minTimestamp >= 0, "Precondition violation - argument 'minTimestamp' must not be negative!");
        checkArgument(maxTimestamp >= 0, "Precondition violation - argument 'maxTimestamp' must not be negative!");
        checkArgument(pageSize > 0, "Precondition violation - argument 'pageSize' must be greater than zero!");
        checkArgument(pageIndex >= 0, "Precondition violation - argument 'pageIndex' must not be negative!");
        checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
        // fail-fast if the period is empty
        if (minTimestamp > maxTimestamp) {
            return Collections.emptyIterator();
        }
        int elementsToSkip = pageSize * pageIndex;
        NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(minTimestamp, true, maxTimestamp, true);
        if (!includeSystemInternalCommits) {
            // apply filter
            subMap = Maps.filterValues(subMap, this::isBinaryUserCommitMetadata);
        }
        final Iterator<Entry<Long, byte[]>> rawIterator;
        switch (order) {
            case ASCENDING:
                rawIterator = subMap.entrySet().iterator();
                break;
            case DESCENDING:
                rawIterator = subMap.descendingMap().entrySet().iterator();
                break;
            default:
                throw new UnknownEnumLiteralException(order);
        }
        // skip entries of the iterator to arrive at the correct page
        for (int i = 0; i < elementsToSkip && rawIterator.hasNext(); i++) {
            rawIterator.next();
        }
        // convert the serialized commit metadata objects into their Object representation
        Iterator<Entry<Long, Object>> iterator = Iterators.transform(rawIterator, this::mapSerialEntryToPair);
        // limit the rest of the iterator to the given page size
        return Iterators.unmodifiableIterator(Iterators.limit(iterator, pageSize));
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataAround(final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        NavigableMap<Long, byte[]> map = this.commitMetadataMap;
        if (!includeSystemInternalCommits) {
            // apply a filter on the map
            map = Maps.filterValues(map, this::isBinaryUserCommitMetadata);
        }
        List<Entry<Long, byte[]>> entriesAround = NavigableMapUtils.entriesAround(map, timestamp, count);
        List<Entry<Long, Object>> resultList = Lists.newArrayList();
        entriesAround.forEach(e -> resultList.add(this.deserializeValueOf(e)));
        resultList.sort(EntryTimestampComparator.INSTANCE.reversed());
        return resultList;
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataBefore(final long timestamp, final int count, final boolean includeSytemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        NavigableMap<Long, byte[]> map = this.commitMetadataMap.headMap(timestamp, false).descendingMap();
        List<Entry<Long, Object>> resultList = Lists.newArrayList();
        for (Entry<Long, byte[]> entry : map.entrySet()) {
            if (resultList.size() >= count) {
                break;
            }
            Entry<Long, Object> deserialized = this.deserializeValueOf(entry);
            if (includeSytemInternalCommits || isUserCommit(deserialized)) {
                resultList.add(deserialized);
            }
        }
        return resultList;
    }

    @Override
    public List<Entry<Long, Object>> getCommitMetadataAfter(final long timestamp, final int count, final boolean includeSystemInternalCommits) {
        checkArgument(timestamp >= 0, "Precondition violation - argument 'timestamp' must not be negative!");
        checkArgument(count >= 0, "Precondition violation - argument 'count' must not be negative!");
        NavigableMap<Long, byte[]> map = this.commitMetadataMap.tailMap(timestamp, false);
        List<Entry<Long, Object>> resultList = Lists.newArrayList();
        for (Entry<Long, byte[]> entry : map.entrySet()) {
            if (resultList.size() >= count) {
                break;
            }
            Entry<Long, Object> deserialized = this.deserializeValueOf(entry);
            if (includeSystemInternalCommits || isUserCommit(deserialized)) {
                resultList.add(deserialized);
            }
        }
        // navigablemaps are sorted in ascending order, we want descending
        return Lists.reverse(resultList);
    }

    @Override
    public int countCommitTimestampsBetween(final long from, final long to, final boolean includeSystemInternalCommits) {
        checkArgument(from >= 0, "Precondition violation - argument 'from' must not be negative!");
        checkArgument(to >= 0, "Precondition violation - argument 'to' must not be negative!");
        // fail-fast if the period is empty
        if (from >= to) {
            return 0;
        }
        NavigableMap<Long, byte[]> subMap = this.commitMetadataMap.subMap(from, true, to, true);
        if (includeSystemInternalCommits) {
            return subMap.size();
        } else {
            // apply filter
            return (int) subMap.entrySet().stream().filter(this::isBinaryUserCommit).count();
        }
    }

    @Override
    public int countCommitTimestamps(final boolean includeSystemInternalCommits) {
        if (includeSystemInternalCommits) {
            return this.commitMetadataMap.size();
        } else {
            // apply filter
            return (int) this.commitMetadataMap.entrySet().stream()
                .filter(this::isBinaryUserCommit)
                .count();
        }
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private boolean isBinaryUserCommit(Entry<Long, byte[]> entry) {
        return isUserCommit(this.deserializeValueOf(entry));
    }

    private boolean isUserCommit(Entry<Long, Object> entry) {
        return isUserCommitMetadata(entry.getValue());
    }

    private boolean isBinaryUserCommitMetadata(final byte[] metadata) {
        if (metadata == null) {
            return true;
        }
        return isUserCommitMetadata(this.deserialize(metadata));
    }

    private boolean isUserCommitMetadata(final Object commitMetadata) {
        return !(commitMetadata instanceof ChronosInternalCommitMetadata);
    }

}
