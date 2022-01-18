package org.chronos.chronodb.internal.api;

import java.util.List;

import org.chronos.chronodb.api.*;
import org.chronos.chronodb.internal.api.index.IndexManagerInternal;
import org.chronos.chronodb.internal.api.query.QueryManager;
import org.chronos.chronodb.internal.api.stream.ChronoDBEntry;
import org.chronos.chronodb.internal.api.stream.CloseableIterator;
import org.chronos.chronodb.internal.impl.dump.CommitMetadataMap;
import org.chronos.common.autolock.ReadWriteAutoLockable;
import org.chronos.common.version.ChronosVersion;

/**
 * An interface that defines methods on {@link ChronoDB} for internal use.
 *
 * <p>
 * If you down-cast a {@link ChronoDB} to a {@link ChronoDBInternal}, you leave the area of the public API, which is strongly discouraged!
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoDBInternal extends ChronoDB, ReadWriteAutoLockable {

	/**
	 * Returns the internal representation of the branch manager associated with this database instance.
	 *
	 * @return The internal representation of the branch manager. Never <code>null</code>.
	 */
	@Override
	public BranchManagerInternal getBranchManager();

	/**
	 * Returns the internal representation of the dateback manager associated with this database.
	 *
	 * @return The internal representation of the dateback manager. Never <code>null</code>.
	 */
	@Override
	public DatebackManagerInternal getDatebackManager();

	@Override
	public IndexManagerInternal getIndexManager();

	/**
	 * A method called by the {@link ChronoDBFactory} after initializing this new instance.
	 *
	 * <p>
	 * This method serves as a startup hook that can be used e.g. to check if any recovery operations are necessary.
	 */
	public void postConstruct();

	/**
	 * Returns the {@link QueryManager} associated with this database instance.
	 *
	 * @return The query manager. Never <code>null</code>.
	 */
	public QueryManager getQueryManager();

	/**
	 * Returns the statistics manager associated with this database instance.
	 *
	 * @return The statistics manager. Never <code>null</code>.
	 */
	public StatisticsManagerInternal getStatisticsManager();

	/**
	 * Creates a transaction on this {@link ChronoDB} based on the given configuration.
	 *
	 * @param configuration
	 *            The configuration to use for transaction construction. Must not be <code>null</code>.
	 *
	 * @return The newly opened transaction. Never <code>null</code>.
	 */
	public ChronoDBTransaction tx(TransactionConfigurationInternal configuration);

	/**
	 * Creates an iterable stream of {@link ChronoDBEntry entries} that reside in this {@link ChronoDB} instance.
	 *
	 * <p>
	 * The resulting iterator will contain entries of <i>all</i> branches.
	 * </p>
	 *
	 * <p>
	 * <b>IMPORTANT:</b> The resulting stream <b>must</b> be {@linkplain CloseableIterator#close() closed} by the caller!
	 *
	 * <p>
	 * Recommended usage pattern is with "try-with-resources":
	 *
	 * <pre>
	 * try (ChronoDBEntryStream stream = chronoDBInternal.entryStream()) {
	 * 	while (stream.hasNext()) {
	 * 		ChronoDBEntry entry = stream.next();
	 * 		// do something with the entry
	 * 	}
	 * } catch (Exception e) {
	 * 	// treat exception
	 * }
	 * </pre>
	 *
	 * @return The stream of entries. Never <code>null</code>, may be empty. Must be closed explicitly.
	 */
	public CloseableIterator<ChronoDBEntry> entryStream();

	/**
	 * Creates an iterable stream of {@link ChronoDBEntry entries} that reside in this {@link ChronoDB} instance.
	 *
	 * <p>
	 * The resulting iterator will contain entries of <i>all</i> branches.
	 * </p>
	 *
	 * <p>
	 * <b>IMPORTANT:</b> The resulting stream <b>must</b> be {@linkplain CloseableIterator#close() closed} by the caller!
	 *
	 * <p>
	 * Recommended usage pattern is with "try-with-resources":
	 *
	 * <pre>
	 * try (ChronoDBEntryStream stream = chronoDBInternal.entryStream(0, 1000L)) {
	 * 	while (stream.hasNext()) {
	 * 		ChronoDBEntry entry = stream.next();
	 * 		// do something with the entry
	 * 	}
	 * } catch (Exception e) {
	 * 	// treat exception
	 * }
	 * </pre>
	 *
	 * @param minTimestamp The minimum timestamp to consider (inclusive). Must not be negative. Must be less than <code>maxTimestamp</code>.
	 * @param maxTimestamp The maximum timestamp to consider (inclusive). Must not be negative. Must be greater than <code>minTimestamp</code>.
	 *
	 * @return The stream of entries. Never <code>null</code>, may be empty. Must be closed explicitly.
	 */
	public CloseableIterator<ChronoDBEntry> entryStream(long minTimestamp, long maxTimestamp);

	/**
	 * Creates an iterable stream of {@link ChronoDBEntry entries} that reside in this {@link ChronoDB} instance.
	 *
	 * <p>
	 * <b>IMPORTANT:</b> The resulting stream <b>must</b> be {@linkplain CloseableIterator#close() closed} by the caller!
	 *
	 * <p>
	 * Recommended usage pattern is with "try-with-resources":
	 *
	 * <pre>
	 * try (ChronoDBEntryStream stream = chronoDBInternal.entryStream("master", 0, 1000L)) {
	 * 	while (stream.hasNext()) {
	 * 		ChronoDBEntry entry = stream.next();
	 * 		// do something with the entry
	 * 	}
	 * } catch (Exception e) {
	 * 	// treat exception
	 * }
	 * </pre>
	 *
	 * @param branch The name of the branch to stream entries from. Must not be <code>null</code>. Must refer to an existing branch.
	 * @param minTimestamp The minimum timestamp to consider (inclusive). Must not be negative. Must be less than <code>maxTimestamp</code>.
	 * @param maxTimestamp The maximum timestamp to consider (inclusive). Must not be negative. Must be greater than <code>minTimestamp</code>.
	 *
	 * @return The stream of entries. Never <code>null</code>, may be empty. Must be closed explicitly.
	 */
	public CloseableIterator<ChronoDBEntry> entryStream(String branch, long minTimestamp, long maxTimestamp);

	/**
	 * Loads the given list of entries into this {@link ChronoDB} instance.
	 *
	 * <p>
	 * Please note that it is assumed that the corresponding branches already exist in the system. This method performs no locking on its own!
	 *
	 * @param entries
	 *            The entries to load. Must not be <code>null</code>, may be empty.
	 */
	public void loadEntries(List<ChronoDBEntry> entries);

	/**
	 * Loads the given commit metadata into this {@link ChronoDB} instance.
	 *
	 * @param commitMetadata
	 *            The commit metadata to read. Must not be <code>null</code>.
	 */
	public void loadCommitTimestamps(CommitMetadataMap commitMetadata);

	/**
	 * Updates the internally stored chronos version to the given version.
	 *
	 * @param chronosVersion
	 *            The new chronos version to store in the DB. Must not be <code>null</code>.
	 *
	 * @since 0.6.0
	 */
	public void updateChronosVersionTo(ChronosVersion chronosVersion);

	/**
	 * Returns the {@link CommitMetadataFilter} instance associated with this database, if any.
	 *
	 * @return The commit metadata filter if present, otherwise <code>null</code>.
	 */
	public CommitMetadataFilter getCommitMetadataFilter();

	/**
	 * Decides whether or not an auto-reindex after reading a DB dump is required or not.
	 *
	 * @return <code>true</code> if an auto-reindex should be performed after reading a dump, otherwise <code>false</code>.
	 */
    public boolean requiresAutoReindexAfterDumpRead();

	/**
	 * Returns the commit timestamp provider of this database.
	 *
	 * @return The commit timestamp provider.
	 */
	public CommitTimestampProvider getCommitTimestampProvider();

}
