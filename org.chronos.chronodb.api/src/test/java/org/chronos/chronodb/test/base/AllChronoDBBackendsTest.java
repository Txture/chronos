package org.chronos.chronodb.test.base;

import com.google.common.collect.Lists;
import org.apache.commons.configuration.Configuration;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.builder.database.ChronoDBPropertyFileBuilder;
import org.chronos.chronodb.api.builder.query.FinalizableQueryBuilder;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.common.logging.ChronoLogger;
import org.junit.After;
import org.junit.Assume;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

public abstract class AllChronoDBBackendsTest extends AllBackendsTest {

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private ChronoDB db;

	// =================================================================================================================
	// GETTERS & SETTERS
	// =================================================================================================================

	protected ChronoDB getChronoDB() {
		if (this.db == null) {
			this.db = this.instantiateChronoDB(this.backend);
		}
		return this.db;
	}

	// =================================================================================================================
	// JUNIT CONTROL
	// =================================================================================================================

	@After
	public void cleanUp() {
		ChronoLogger.logDebug("Closing ChronoDB on backend '" + this.backend + "'.");
		if (this.db != null) {
			if (this.db.isClosed() == false) {
				this.db.close();
			}
			this.db = null;
		}
	}

	// =================================================================================================================
	// UTILITY
	// =================================================================================================================

	protected ChronoDB instantiateChronoDB(final String backend) {
		Configuration configuration = this.createChronosConfiguration(backend);
		return this.createDB(configuration);
	}

	protected ChronoDB createDB(final Configuration configuration) {
		checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
		ChronoDBPropertyFileBuilder builder = ChronoDB.FACTORY.create().fromConfiguration(configuration);
		this.applyExtraTestMethodProperties(configuration);
		return builder.build();
	}


	protected ChronoDB reinstantiateDB() {
		if (this.db == null) {
			return this.getChronoDB();
		}
		ChronoLogger.logDebug("Reinstantiating ChronoDB on backend '" + this.backend + "'.");
		this.db.close();
		this.db = this.instantiateChronoDB(this.backend);
		return this.db;
	}

	protected ChronoDB closeAndReopenDB() {
		ChronoDB db = this.getChronoDB();
		// this won't work for in-memory (obviously)
		Assume.assumeTrue(db.getFeatures().isPersistent());
		ChronoDBConfiguration configuration = db.getConfiguration();
		db.close();
		this.db = ChronoDB.FACTORY.create().fromConfiguration(configuration.asCommonsConfiguration()).build();
		return this.db;
	}

	protected TemporalKeyValueStore getMasterTkvs(final ChronoDB db) {
		return this.getTkvs(db, ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	protected TemporalKeyValueStore getTkvs(final ChronoDB db, final String branchName) {
		Branch branch = db.getBranchManager().getBranch(branchName);
		return ((BranchInternal) branch).getTemporalKeyValueStore();
	}

	protected void assumeRolloverIsSupported(final ChronoDB db) {
		Assume.assumeTrue(db.getFeatures().isRolloverSupported());
	}

	protected void assumeIsPersistent(final ChronoDB db){
		Assume.assumeTrue(db.getFeatures().isPersistent());
	}

	protected void assumeIncrementalBackupIsSupported(final ChronoDB db){
		Assume.assumeTrue(db.getFeatures().isIncrementalBackupSupported());
	}

	/**
	 * Asserts that the set of keys (given by the initial varargs arguments) is equal to the result of the query (last
	 * vararg argument).
	 *
	 * <p>
	 * Example:
	 *
	 * <pre>
	 * // assert that the default keyspace consists of the Set {a, b}
	 * assertKeysEqual("a", "b", db.tx().find().inDefaultKeyspace());
	 * </pre>
	 *
	 * @param objects
	 *            The varargs objects. The last argument is the query, all elements before are the expected result keys.
	 */
	@SuppressWarnings("unchecked")
	public static void assertKeysEqual(final Object... objects) {
		List<Object> list = Lists.newArrayList(objects);
		Object last = list.get(list.size() - 1);
		Set<String> keySet = null;
		if (last instanceof Set) {
			keySet = ((Set<QualifiedKey>) last).stream().map(QualifiedKey::getKey).collect(Collectors.toSet());
		} else if (last instanceof FinalizableQueryBuilder) {
			keySet = ((FinalizableQueryBuilder) last).getKeysAsSet().stream().map(QualifiedKey::getKey)
					.collect(Collectors.toSet());
		} else {
			fail("Last element of 'assertKeysEqual' varargs must either be a FinalizableQueryBuilder or a Set<QualifiedKey>!");
		}
		Set<String> keys = list.subList(0, list.size() - 1).stream().map(k -> (String) k).collect(Collectors.toSet());
		assertEquals(keys, keySet);
	}

}
