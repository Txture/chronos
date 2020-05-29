package org.chronos.chronodb.api;

import com.google.common.annotations.VisibleForTesting;
import org.chronos.chronodb.api.exceptions.DatebackException;
import org.chronos.chronodb.api.key.QualifiedKey;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

public interface Dateback {

    /**
     * Marker object. Used by several transformation methods to indicate that the transformation does not change the
     * value at hand.
     */
    public static final Object UNCHANGED = new Object();

    /**
     * Purges the entry at the given coordinates from the store, eliminating it completely from the history.
     *
     * @param keyspace  The keyspace of the entry to purge. Must not be <code>null</code>.
     * @param key       The key of the entry to purge. Must not be <code>null</code>.
     * @param timestamp The (exact) timestamp of the entry to purge. Must not be negative.
     * @return <code>true</code> if the entry was purged successfully from the store, or <code>false</code> if there was
     * no entry to purge at the given coordinates.
     */
    public boolean purgeEntry(String keyspace, String key, long timestamp);

    /**
     * Purges the given key from the store, eliminating it completely from the history.
     *
     * <p>
     * In contrast to {@link #purgeEntry(String, String, long)}, this method will remove ALL occurrences of the key. In
     * other words, after calling this method, the history of the key will be empty.
     *
     * @param keyspace The keyspace in which the key resides. Must not be <code>null</code>.
     * @param key      The key to (unconditionally) purge from the store. Must not be <code>null</code>.
     */
    public void purgeKey(String keyspace, String key);

    /**
     * Purges all entries with the given key from the store which match the given predicate, eliminating them completely
     * from the history.
     *
     * <p>
     * In contrast to {@link #purgeEntry(String, String, long)}, this method can potentially remove multiple occurrences
     * of the key.
     *
     * @param keyspace  The keyspace where the key resides. Must not be <code>null</code>.
     * @param key       The key to purge from the store. Must not be <code>null</code>.
     * @param predicate The predicate to use for deciding whether to purge an entry or not. Must not be <code>null</code>.
     *                  Must be a pure, side-effect free function. The first parameter is the timestamp of the entry at hand,
     *                  the second parameter is the value associated with the key at this timestamp (which may be
     *                  <code>null</code> to indicate that this entry is a deletion). The function should return
     *                  <code>true</code> if the entry should be purged, or <code>false</code> if the entry should remain
     *                  untouched.
     */
    public void purgeKey(String keyspace, String key, BiPredicate<Long, Object> predicate);

    /**
     * Purges all entries with the given key from the store that reside in the given time range, eliminating them
     * completely from the history.
     *
     * @param keyspace        The keyspace where the key resides. Must not be <code>null</code>.
     * @param key             The key to purge from the store. Must not be <code>null</code>.
     * @param purgeRangeStart The lower bound of the range to purge entries from (inclusive). Must not be negative, must not be
     *                        larger than <code>purgeRangeEnd</code>.
     * @param purgeRangeEnd   The upper bound of the range to purge entries from (inclusive). Must not be negative, must be greater
     *                        than or equal to <code>purgeRangeStart</code>.
     */
    public void purgeKey(String keyspace, String key, long purgeRangeStart, long purgeRangeEnd);

    /**
     * Purges all entries in the given keyspace that reside in the given time range, eliminating them completely from the history.
     *
     * @param keyspace The keyspace to purge the entries from. Must not be <code>null</code>.
     * @param purgeRangeStart The lower bound of the range to purge entries from (inclusive). Must not be negative, must not be
     *                        larger than <code>purgeRangeEnd</code>.
     * @param purgeRangeEnd   The upper bound of the range to purge entries from (inclusive). Must not be negative, must be greater
     *                        than or equal to <code>purgeRangeStart</code>.
     */
    public void purgeKeyspace(String keyspace, long purgeRangeStart, long purgeRangeEnd);

    /**
     * Purges the given commit from the store, eliminating it (and its associated changes) completely from the history.
     *
     * <p>
     * This will also delete the commit metadata associated with the commit.
     *
     * @param commitTimestamp The (exact) timestamp of the commit to purge. Must not be negative.
     */
    public void purgeCommit(long commitTimestamp);

    /**
     * Purges all commits in the given time range (and their associated changes and metadata), eliminating them
     * completely from the store.
     *
     * @param purgeRangeStart The lower bound of the timestamp range to perform the purging in (inclusive). Must not be negative,
     *                        must not be larger than <code>purgeRangeEnd</code>.
     * @param purgeRangeEnd   The upper bound of the timestamp range to perform the purging in (inclusive). Must not be negative,
     *                        must not be smaller than <code>purgeRangeStart</code>.
     */
    public void purgeCommits(long purgeRangeStart, long purgeRangeEnd);

    /**
     * Injects an entry with the given properties into the store.
     *
     * <p>
     * Please keep in mind the following properties of this operation:
     * <ul>
     * <li>If an entry existed at the specified coordinates, this entry will be <b>overwritten</b>.
     * <li>Using <code>null</code> as the <code>value</code> parameter inserts a "deletion" entry.
     * <li>This method can potentially introduce a new commit in the commit log.
     * <li>This method can <b>not</b> be used to inject entries in the future.
     * <li>This method can <b>not</b> be used to inject entries historically before the branching timestamp of the
     * current branch.
     * <li>This method <b>can</b> be used to create new keyspaces by specifying a previously unused keyspace name.
     * <li>This method <b>can</b> be used to inject entries after the current "now" timestamp, but not after
     * {@link System#currentTimeMillis()}. Injecting entries after "now" will advance the "now" timestamp.
     * </ul>
     *
     * @param keyspace  The keyspace in which the new entry should be created. Must not be <code>null</code>. If the keyspace
     *                  did not exist before, it will be created.
     * @param key       The key at which the new entry should be created. Must not be <code>null</code>.
     * @param timestamp The timestamp at which the new entry should be inserted. Any existing entry at this timestamp will be
     *                  overwritten. Must not be negative. Must be larger than the branching timestamp of this branch. Must
     *                  not be in the future.
     * @param value     The value to insert. May be <code>null</code> to indicate that the key was deleted from the store at
     *                  these coordinates.
     */
    public void inject(String keyspace, String key, long timestamp, Object value);

    /**
     * Injects an entry with the given properties into the store.
     *
     * <p>
     * Please keep in mind the following properties of this operation:
     * <ul>
     * <li>If an entry existed at the specified coordinates, this entry will be <b>overwritten</b>.
     * <li>Using <code>null</code> as the <code>value</code> parameter inserts a "deletion" entry.
     * <li>This method can potentially introduce a new commit in the commit log.
     * <li>This method can <b>not</b> be used to inject entries in the future.
     * <li>This method can <b>not</b> be used to inject entries historically before the branching timestamp of the
     * current branch.
     * <li>This method <b>can</b> be used to create new keyspaces by specifying a previously unused keyspace name.
     * <li>This method <b>can</b> be used to inject entries after the current "now" timestamp, but not after
     * {@link System#currentTimeMillis()}. Injecting entries after "now" will advance the "now" timestamp.
     * </ul>
     *
     * @param keyspace       The keyspace in which the new entry should be created. Must not be <code>null</code>. If the keyspace
     *                       did not exist before, it will be created.
     * @param key            The key at which the new entry should be created. Must not be <code>null</code>.
     * @param timestamp      The timestamp at which the new entry should be inserted. Any existing entry at this timestamp will be
     *                       overwritten. Must not be negative. Must be larger than the branching timestamp of this branch. Must
     *                       not be in the future.
     * @param value          The value to insert. May be <code>null</code> to indicate that the key was deleted from the store at
     *                       these coordinates.
     * @param commitMetadata The commit metadata to use for the entry. Will override pre-existing commit metadata, in case that
     *                       there was a commit at the given timestamp. May be <code>null</code>.
     */
    public void inject(String keyspace, String key, long timestamp, Object value, Object commitMetadata);

    /**
     * Injects an entry with the given properties into the store.
     *
     * <p>
     * Please keep in mind the following properties of this operation:
     * <ul>
     * <li>If an entry existed at the specified coordinates, this entry will be <b>overwritten</b>.
     * <li>Using <code>null</code> as the <code>value</code> parameter inserts a "deletion" entry.
     * <li>This method can potentially introduce a new commit in the commit log.
     * <li>This method can <b>not</b> be used to inject entries in the future.
     * <li>This method can <b>not</b> be used to inject entries historically before the branching timestamp of the
     * current branch.
     * <li>This method <b>can</b> be used to create new keyspaces by specifying a previously unused keyspace name.
     * <li>This method <b>can</b> be used to inject entries after the current "now" timestamp, but not after
     * {@link System#currentTimeMillis()}. Injecting entries after "now" will advance the "now" timestamp.
     * </ul>
     *
     * @param keyspace               The keyspace in which the new entry should be created. Must not be <code>null</code>. If the keyspace
     *                               did not exist before, it will be created.
     * @param key                    The key at which the new entry should be created. Must not be <code>null</code>.
     * @param timestamp              The timestamp at which the new entry should be inserted. Any existing entry at this timestamp will be
     *                               overwritten. Must not be negative. Must be larger than the branching timestamp of this branch. Must
     *                               not be in the future.
     * @param value                  The value to insert. May be <code>null</code> to indicate that the key was deleted from the store at
     *                               these coordinates.
     * @param commitMetadata         The commit metadata to use for the entry. May be <code>null</code>.
     * @param overrideCommitMetadata Decides whether or not to override pre-existing commit metadata (if there is a commit at the given
     *                               timestamp). Use <code>true</code> to override existing metadata, or <code>false</code> to keep it
     *                               intact if it exists. If no commit metadata exists at the given timestamp, this parameter will be
     *                               ignored.
     */
    public void inject(String keyspace, String key, long timestamp, Object value, Object commitMetadata,
                       boolean overrideCommitMetadata);

    /**
     * Injects multiple entries at the given timestamp into the store.
     *
     * <p>
     * This is a multiplicity-many version of {@link #inject(String, String, long, Object)}; the same restrictions
     * regarding the entries apply here.
     *
     * @param timestamp The timestamp at which to inject the entries. Must not be negative. Must be greater than the branching
     *                  timestamp of the current branch. Must not be in the future.
     * @param entries   The entries to insert at the given timestamp. The map entries contain the keyspace/key combination in
     *                  the first component and the corresponding value in the second component. <code>null</code> is a legal
     *                  entry value and can be used to indicate a deletion in the store. The map itself must not be
     *                  <code>null</code>.
     */
    public void inject(long timestamp, Map<QualifiedKey, Object> entries);

    /**
     * Injects multiple entries at the given timestamp into the store.
     *
     * <p>
     * This is a multiplicity-many version of {@link #inject(String, String, long, Object)}; the same restrictions
     * regarding the entries apply here.
     *
     * @param timestamp      The timestamp at which to inject the entries. Must not be negative. Must be greater than the branching
     *                       timestamp of the current branch. Must not be in the future.
     * @param entries        The entries to insert at the given timestamp. The map entries contain the keyspace/key combination in
     *                       the first component and the corresponding value in the second component. <code>null</code> is a legal
     *                       entry value and can be used to indicate a deletion in the store. The map itself must not be
     *                       <code>null</code>.
     * @param commitMetadata The commit metadata to store for this timestamp. This will override pre-existing metadata, if there
     *                       has been a previous commit on this timestamp.
     */
    public void inject(long timestamp, Map<QualifiedKey, Object> entries, Object commitMetadata);

    /**
     * Injects multiple entries at the given timestamp into the store.
     *
     * <p>
     * This is a multiplicity-many version of {@link #inject(String, String, long, Object)}; the same restrictions
     * regarding the entries apply here.
     *
     * @param timestamp              The timestamp at which to inject the entries. Must not be negative. Must be greater than the branching
     *                               timestamp of the current branch. Must not be in the future.
     * @param entries                The entries to insert at the given timestamp. The map entries contain the keyspace/key combination in
     *                               the first component and the corresponding value in the second component. <code>null</code> is a legal
     *                               entry value and can be used to indicate a deletion in the store. The map itself must not be
     *                               <code>null</code>.
     * @param commitMetadata         The commit metadata to store for this timestamp. This will override pre-existing metadata, if there
     *                               has been a previous commit on this timestamp.
     * @param overrideCommitMetadata Decides whether or not to override pre-existing commit metadata (if there is a commit at the given
     *                               timestamp). Use <code>true</code> to override existing metadata, or <code>false</code> to keep it
     *                               intact if it exists. If no commit metadata exists at the given timestamp, this parameter will be
     *                               ignored.
     */
    public void inject(long timestamp, Map<QualifiedKey, Object> entries, Object commitMetadata,
                       boolean overrideCommitMetadata);

    /**
     * Transforms the entry at the given coordinates by applying the given transformation function.
     *
     * <p>
     * Please note that the transformation function may return the {@link Dateback#UNCHANGED} constant to indicate that
     * the transformation does not change the value.
     *
     * @param keyspace       The keyspace of the entry to transform. Must not be <code>null</code>.
     * @param key            The key of the entry to transform. Must not be <code>null</code>.
     * @param timestamp      The timestamp of the entry to transform. Must match the entry <b>exactly</b>. Must not be negative.
     * @param transformation The transformation to apply. Must not be <code>null</code>. May return {@link Dateback#UNCHANGED} to
     *                       indicate that the value should remain unchanged. The passed value may be <code>null</code> to indicate
     *                       that the entry marks a deletion. The function has to be a pure function, i.e. free of side effects and
     *                       depending only on the passed parameters.
     */
    public void transformEntry(String keyspace, String key, long timestamp, Function<Object, Object> transformation);

    /**
     * Transforms all values of the given key by applying the given transformation function.
     *
     * <p>
     * Please note that the transformation function may return the {@link Dateback#UNCHANGED} constant to indicate that
     * the transformation does not change the value.
     *
     * @param keyspace       The keyspace of the entries to transform. Must not be <code>null</code>.
     * @param key            The key of the entries to transform. Must not be <code>null</code>.
     * @param transformation The transformation function to apply. It receives the timestamp and the value of the entry as
     *                       parameters and should return the new value to assign to the given timestamp. The function may return
     *                       <code>null</code> in order to indicate a deletion. The passed value may be <code>null</code> to
     *                       indicate that the entry marks a deletion. The function should return {@link Dateback#UNCHANGED} to
     *                       indicate that the current entry remains unchanged. The function has to be a pure function, i.e. free
     *                       of side effects and depending only on the passed parameters.
     */
    public void transformValuesOfKey(String keyspace, String key, BiFunction<Long, Object, Object> transformation);

    /**
     * Transforms all entries that belong to a given commit.
     *
     * <p>
     * The transformation function <b>may</b>:
     * <ul>
     * <li>Add new entries to the map. Those will be injected into the commit.
     * <li>Remove entries from the map. Those will be removed from the commit.
     * <li>Change the values associated with keys. Those will be updated.
     * <li>Assign the {@link Dateback#UNCHANGED} value to a key. This will leave the entry untouched in the commit.
     * </ul>
     *
     * The map passed to the transformation function as parameter is <b>immutable</b>. The transformation function
     * should construct a new map internally and return it.
     *
     * @param commitTimestamp The (precise) timestamp of the commit to transform. Must not be negative. If there is no commit at the
     *                        specified timestamp, this method is a no-op and will return immediately.
     * @param transformation  The transformation function to apply. Must not be <code>null</code>. Must be a pure, side-effect free
     *                        function.
     */
    public void transformCommit(long commitTimestamp,
                                Function<Map<QualifiedKey, Object>, Map<QualifiedKey, Object>> transformation);

    /**
     * Transforms the values of the given keyspace.
     *
     * <p>
     * This method <b>is allowed</b> to:
     * <ul>
     *     <li>Update existing values</li>
     *     <li>Leave existing values the same</li>
     * </ul>
     * </p>
     *
     * <p>
     * This method is <b>not allowed</b> to:
     * <ul>
     *     <li>Replace a value with a deletion marker</li>
     *     <li>Delete existing entries</li>
     *     <li>Introduce new entries at coordinates which previously had no entry</li>
     * </ul>
     *
     * </p>
     *
     * @param keyspace The keyspace to transform. Must not be <code>null</code>.
     * @param valueTransformation The value transformation to apply. Must not be <code>null</code>.
     */
    public void transformValuesOfKeyspace(String keyspace, KeyspaceValueTransformation valueTransformation);

    /**
     * Updates the metadata of the given commit, replacing it with the given one.
     *
     * @param commitTimestamp The timestamp of the commit to update the metadata for. Must not be <code>null</code>, must refer to
     *                        an existing commit in the store.
     * @param newMetadata     The new metadata object to assign to the commit. May be <code>null</code>.
     * @throws DatebackException Thrown if there is no commit in the store at the given timestamp.
     */
    public void updateCommitMetadata(long commitTimestamp, Object newMetadata);

    /**
     * Performs cleanup operations on the underlying store (if necessary).
     *
     * <p>
     * Users can call this operation in order to flush changes into the store and freeing RAM.
     *
     * <p>
     * Cleanup will also be performed automatically at the end of a dateback process.
     */
    public void cleanup();


    /**
     * Returns the highest timestampthat is not yet affected by changes performed by the dateback process.
     *
     * @return The highest unaffected timestamp. May be -1 if all timestamps have been affected.
     */
    public long getHighestUntouchedTimestamp();

    /**
     * Returns the value at the given coordinates (just like {@link ChronoDBTransaction#get(String, String)}).
     *
     * @param timestamp The timestamp at which to perform the lookup. Must be positive, must be less than or equal to {@link #getHighestUntouchedTimestamp()}.
     * @param keyspace  The keyspace to search in. Must not be <code>null</code>.
     * @param key       The key to search for. Must not be <code>null</code>.
     * @return The object located at the given coordinates, or <code>null</code> if no object was found.
     */
    public Object get(long timestamp, String keyspace, String key);

    /**
     * Returns the keyset at the given timestamp.
     *
     * @param timestamp The timestamp at which to calculate the keyset. Must be positive, must be less than or equal to {@link #getHighestUntouchedTimestamp()}.
     * @param keyspace  The keyspace to calculate the keyset for. Must not be <code>null</code>.
     * @return The keyset. May be empty, but never <code>null</code>.
     */
    public Set<String> keySet(long timestamp, String keyspace);

    /**
     * Returns the keyspaces which existed at the given timestamp.
     *
     * <p>
     * Works similarly to {@link ChronoDBTransaction#keyspaces()}.
     * </p>
     *
     * @param timestamp The timestamp at which to calculate the set of keyspaces. Must be positive, must be less than or equal to {@link #getHighestUntouchedTimestamp()}.
     * @return The set of keyspaces at the given timestamp. May be empty, but never <code>null</code>.
     */
    public Set<String> keyspaces(long timestamp);

    /**
     * For testing purposes only. Checks if this dateback instance is still accessible or already closed.
     *
     * @return <code>true</code> if this dateback instance has already been closed, or <code>false</code> if it is still
     * open.
     */
    @VisibleForTesting
    public boolean isClosed();


    public interface KeyspaceValueTransformation {

        /**
         * Transforms the value at the given coordinates.
         *
         * @param key The key which holds the value. Never <code>null</code>.
         * @param timestamp The timestamp at which the operation occurs.
         * @param oldValue The old value. Never <code>null</code>.
         * @return The new value. Use {@link #UNCHANGED} if you want to keep the old value. Never <code>null</code>.
         */
        public Object transformValue(String key, long timestamp, Object oldValue);

    }
}
