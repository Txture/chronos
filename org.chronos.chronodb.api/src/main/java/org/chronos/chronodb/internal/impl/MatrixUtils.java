package org.chronos.chronodb.internal.impl;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.chronos.chronodb.api.BranchHeadStatistics;
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalKey;

import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class MatrixUtils {

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    protected MatrixUtils() {
        throw new IllegalStateException("MatrixMap must not be instantiated!");
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    /**
     * Generates and returns a random name for a matrix map.
     *
     * @return A random matrix map name. Never <code>null</code>.
     */
    public static String generateRandomName() {
        return "MATRIX_" + UUID.randomUUID().toString().replace("-", "_");
    }

    /**
     * Checks if the given string is a valid matrix map name.
     *
     * @param mapName The map name to check. May be <code>null</code>.
     * @return <code>true</code> if the given map name is a valid matrix map name, or <code>false</code> if it is syntactically invalid or <code>null</code>.
     */
    public static boolean isValidMatrixTableName(final String mapName) {
        if (mapName == null) {
            return false;
        }
        String tablenNameRegex = "MATRIX_[a-zA-Z0-9_]+";
        return mapName.matches(tablenNameRegex);
    }

    /**
     * Asserts that the given map name is a valid matrix map name.
     *
     * <p>
     * If the map name matches the syntax, this method does nothing. Otherwise, an exception is thrown.
     *
     * @param mapName The map name to verify.
     * @throws NullPointerException     Thrown if the map name is <code>null</code>.
     * @throws IllegalArgumentException Thrown if the map name is no syntactically valid matrix table name.
     * @see #isValidMatrixTableName(String)
     */
    public static void assertIsValidMatrixTableName(final String mapName) {
        if (mapName == null) {
            throw new IllegalArgumentException("NULL is no valid Matrix Map name!");
        }
        if (isValidMatrixTableName(mapName) == false) {
            throw new IllegalArgumentException("The map name '" + mapName + "' is no valid Matrix Map name!");
        }
    }

    /**
     * Calculates the head statistics given an iterator over the keys (in ascending order) and a function to check if an entry is a deletion.
     *
     * @param ascendingKeyIterator The iterator to use. Must return keys in ascending order. Must not be <code>null</code>.
     * @param isDeletionCheck      A function to check if the entry for a given temporal key represents a deletion. Must not be <code>null</code>. Must return <code>true</code> if the entry for a given temporal key represents a deletion, or <code>false</code> if the entry represents an insert or an update.
     * @return The head statistics. Never <code>null</code>.
     */
    public static BranchHeadStatistics calculateBranchHeadStatistics(Iterator<UnqualifiedTemporalKey> ascendingKeyIterator, Function<UnqualifiedTemporalKey, Boolean> isDeletionCheck) {
        checkNotNull(ascendingKeyIterator, "Precondition violation - argument 'ascendingKeyIterator' must not be NULL!");
        checkNotNull(isDeletionCheck, "Precondition violation - argument 'isDeletionCheck' must not be NULL!");
        long totalEntries = 0;
        long entriesInHead = 0;
        PeekingIterator<UnqualifiedTemporalKey> iterator = Iterators.peekingIterator(ascendingKeyIterator);
        while (iterator.hasNext()) {
            UnqualifiedTemporalKey entryKey = iterator.next();
            UnqualifiedTemporalKey nextEntry = iterator.hasNext() ? iterator.peek() : null;
            if (isLatestEntryForKey(entryKey, nextEntry)) {
                // this is the latest entry in the history of this user key.
                if (isDeletionCheck.apply(entryKey)) {
                    // the last entry is a deletion, it does not contribute to the
                    // head revision (only to the history)
                    totalEntries++;
                } else {
                    // the last entry is a valid key-value pair in the head revision.
                    entriesInHead++;
                    totalEntries++;
                }
            } else {
                // the next entry in the matrix also references the same user key,
                // so this entry is part of the history and does not contribute to
                // the head version.
                totalEntries++;
            }
        }
        return new BranchHeadStatisticsImpl(entriesInHead, totalEntries);
    }

    private static boolean isLatestEntryForKey(final UnqualifiedTemporalKey entryKey, final UnqualifiedTemporalKey nextKey) {
        if (nextKey == null) {
            // the matrix has no next entry, therefore the current entry is the
            // last in the history of this key
            return true;
        }
        if (!Objects.equals(entryKey.getKey(), nextKey.getKey())) {
            // the next entry in the matrix is about a different user key, therefore
            // the current entry is the last in the history of our user key.
            return true;
        }
        return false;
    }
}