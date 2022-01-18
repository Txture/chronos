package org.chronos.chronodb.inmemory;

import org.chronos.chronodb.api.TextCompare;
import org.chronos.chronodb.internal.api.Period;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public class IndexEntry implements Comparable<IndexEntry> {

    public static Comparator<IndexEntry> createComparator(TextCompare textCompare) {
        return (o1, o2) -> {
            if (o1 == null && o2 == null) {
                return 0;
            } else if (o1 == null && o2 != null) {
                return -1;
            } else if (o1 != null && o2 == null) {
                return 1;
            } else {
                return textCompare.apply(o1.getKey()).compareTo(textCompare.apply(o2.getKey()));
            }
        };
    }

    private final IndexKey key;
    private final List<Period> validPeriods;

    public IndexEntry(IndexKey key, List<Period> validPeriods) {
        checkNotNull(key, "Precondition violation - argument 'key' must not be NULL!");
        checkNotNull(validPeriods, "Precondition violation - argument 'validPeriods' must not be NULL!");
        this.key = key;
        this.validPeriods = Collections.unmodifiableList(validPeriods);
    }

    public IndexKey getKey() {
        return key;
    }

    public List<Period> getValidPeriods() {
        return validPeriods;
    }


    @Override
    public int compareTo(@NotNull final IndexEntry o) {
        return this.getKey().compareTo(o.getKey());
    }

    @Override
    public String toString() {
        return "IndexEntry{key=" + key + ", validPeriods=" + validPeriods + '}';
    }
}
