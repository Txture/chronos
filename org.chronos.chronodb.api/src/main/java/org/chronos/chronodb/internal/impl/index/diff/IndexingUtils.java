package org.chronos.chronodb.internal.impl.index.diff;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.SecondaryIndex;

import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class IndexingUtils {

    public static void assertIsValidIndexName(String indexName) {
        checkNotNull(indexName, "Precondition violation - argument 'indexName' must not be NULL!");
        if (indexName.isEmpty()) {
            throw new IllegalArgumentException("The given 'indexName' is blank!");
        }
        String trimmed = indexName.trim();
        if (trimmed.equals(indexName) == false) {
            throw new IllegalArgumentException("The given 'indexName' starts or ends with whitespace, which is not allowed!");
        }
        if(indexName.contains("\n")){
            throw new IllegalArgumentException("The given 'indexName' contains line breaks / newlines, which are not allowed!");
        }
        if(indexName.length() > 255){
            throw new IllegalArgumentException("The given 'indexName' is too long (maximum allowed is 255 characters, your index name contains " + indexName.length() + " characters).");
        }
    }

    /**
     * Calculates a diff between the indexed values of the given <code>oldValue</code> and <code>newValue</code>.
     *
     * @param indices  A mapping from index names to indexers. This method will attempt to resolve the index value(s) for all
     *                 index names. May be empty, in which case the resulting diff will also be empty. Must not be
     *                 <code>null</code>.
     * @param oldValue The old (previous) value object to analyze. May be <code>null</code> to indicate that the
     *                 <code>newValue</code> is an addition.
     * @param newValue The new value object to analyze. May be <code>null</code> to indicate that the <code>oldValue</code>
     *                 was deleted.
     * @return The diff between the old and new index values. Never <code>null</code>, may be empty.
     */
    public static IndexValueDiff calculateDiff(final Set<SecondaryIndex> indices,
                                               final Object oldValue, final Object newValue) {
        checkNotNull(indices, "Precondition violation - argument 'indices' must not be NULL!");
        MutableIndexValueDiff diff = new MutableIndexValueDiff(oldValue, newValue);
        if (indices.isEmpty() || oldValue == null && newValue == null) {
            // return the empty diff
            return diff;
        }
        // iterate over the known indices
        for (SecondaryIndex index : indices) {
            // prepare the sets of values for that index
            Set<Comparable<?>> oldValues = index.getIndexedValuesForObject(oldValue);
            Set<Comparable<?>> newValues = index.getIndexedValuesForObject(newValue);
            // calculate the set differences
            Set<Object> addedValues = Sets.newHashSet();
            addedValues.addAll(newValues);
            addedValues.removeAll(oldValues);
            Set<Object> removedValues = Sets.newHashSet();
            removedValues.addAll(oldValues);
            removedValues.removeAll(newValues);
            // update the diff
            for (Object addedValue : addedValues) {
                diff.add(index, addedValue);
            }
            diff.removeMultipleValues(index, removedValues);
        }
        return diff;
    }

    /**
     * Returns the indexed values for the given object.
     *
     * @param indices A multimap from index name to indexers. Must not be <code>null</code>, may be empty, in which case the
     *                resulting multimap will be empty.
     * @param object  The object to get the index values for. May be <code>null</code>, in which case the resulting multimap
     *                will be empty.
     * @return The mapping from index name to indexed values. May be empty, but never <code>null</code>.
     */
    public static SetMultimap<SecondaryIndex, Object> getIndexedValuesForObject(
        final Set<SecondaryIndex> indices, final Object object) {
        checkNotNull(indices, "Precondition violation - argument 'indices' must not be NULL!");
        SetMultimap<SecondaryIndex, Object> indexValuesMap = HashMultimap.create();
        if (object == null || indices.isEmpty()) {
            return indexValuesMap;
        }
        for (SecondaryIndex index : indices) {
            Set<Comparable<?>> indexValues = index.getIndexedValuesForObject(object);
            if (indexValues.isEmpty()) {
                continue;
            }
            indexValuesMap.putAll(index, indexValues);
        }
        return indexValuesMap;
    }

    public static boolean isValidIndexValue(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof String) {
            String s = (String) obj;
            if (s.trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }

}
