package org.chronos.chronodb.test.cases.engine.dateback;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.dateback.log.DatebackOperation;
import org.chronos.chronodb.internal.api.dateback.log.IPurgeKeyspaceOperation;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class RolloverDatebackTest extends AllChronoDBBackendsTest {

    @Test
    public void injectionIsTransferredToFutureChunks() {
        ChronoDB db = this.getChronoDB();
        this.assumeRolloverIsSupported(db);
        List<Long> chunkStartTimestamps = createFourRolloverModel(db);
        // inject a new key into the second chunk
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "C", chunkStartTimestamps.get(1) + 1, 1);
        });

        // C should be accessible in HEAD
        assertEquals(1, (int) db.tx().get("C"));
        // C should have one history entry (from the inject, rollovers don't count)
        assertEquals(1, Iterators.size(db.tx().history("C")));
    }

    @Test
    public void purgedEntriesAreRemovedFromRollover() {
        ChronoDB db = this.getChronoDB();
        this.assumeRolloverIsSupported(db);
        List<Long> chunkStartTimestamps = createFourRolloverModel(db);

        // get the first two commits of A
        List<Long> commitTimestampsOfA = Lists.reverse(Lists.newArrayList(db.tx().history("A")));
        long firstCommitOfA = commitTimestampsOfA.get(0);
        long secondCommitOfA = commitTimestampsOfA.get(1);
        assertTrue(firstCommitOfA < secondCommitOfA);
        assertTrue(secondCommitOfA < chunkStartTimestamps.get(1));

        // purge the first two commits of A from the store
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.purgeEntry(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "A", firstCommitOfA);
            dateback.purgeEntry(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "A", secondCommitOfA);
        });

        List<Long> newHistorOfA = Lists.newArrayList(db.tx().history("A"));
        assertEquals(4, newHistorOfA.size());

    }

    @Test
    public void canPurgeKeyspace() throws Exception {
        ChronoDB db = this.getChronoDB();
        this.assumeIncrementalBackupIsSupported(db);
        long commit1;
        long commit2;
        long commit3;
        long commit4;
        long commit5;
        { // commit 1
            ChronoDBTransaction tx = db.tx();
            tx.put("mykeyspace", "a", "a1");
            tx.put("mykeyspace", "b", "b1");
            tx.put("mykeyspace", "c", "c1");
            tx.put("test", "x", 1);
            commit1 = tx.commit();
        }
        Thread.sleep(1);
        db.getMaintenanceManager().performRolloverOnAllBranches();
        Thread.sleep(1);
        { // commit 2
            ChronoDBTransaction tx = db.tx();
            tx.put("mykeyspace", "a", "a2");
            tx.remove("mykeyspace", "c");
            tx.put("mykeyspace", "d", "d1");
            tx.put("test", "x", 2);
            commit2 = tx.commit();
        }
        { // commit 3
            ChronoDBTransaction tx = db.tx();
            tx.put("mykeyspace", "a", "a3");
            tx.put("mykeyspace", "d", "d2");
            tx.put("test", "x", 3);
            commit3 = tx.commit();
        }
        Thread.sleep(1);
        db.getMaintenanceManager().performRolloverOnAllBranches();
        Thread.sleep(1);
        { // commit 4
            ChronoDBTransaction tx = db.tx();
            tx.put("mykeyspace", "a", "a4");
            tx.put("mykeyspace", "d", "d3");
            tx.put("test", "x", 4);
            commit4 = tx.commit();
        }
        Thread.sleep(1);
        db.getMaintenanceManager().performRolloverOnAllBranches();
        Thread.sleep(1);
        { // commit 5
            ChronoDBTransaction tx = db.tx();
            tx.remove("mykeyspace", "a");
            tx.put("mykeyspace", "d", "d4");
            tx.put("test", "x", 5);
            commit5 = tx.commit();
        }
        Thread.sleep(1);
        db.getMaintenanceManager().performRolloverOnAllBranches();
        Thread.sleep(1);

        db.getDatebackManager().datebackOnMaster(dateback -> dateback.purgeKeyspace("mykeyspace", commit2, commit4));

        List<DatebackOperation> allOps = db.getDatebackManager().getAllPerformedDatebackOperations();
        assertThat(allOps.size(), is(1));
        DatebackOperation operation = Iterables.getOnlyElement(allOps);
        assertThat(operation, is(instanceOf(IPurgeKeyspaceOperation.class)));
        IPurgeKeyspaceOperation op = (IPurgeKeyspaceOperation) operation;
        assertThat(op.getFromTimestamp(), is(commit2));
        assertThat(op.getToTimestamp(), is(commit4));
        assertThat(op.getKeyspace(), is("mykeyspace"));

        // check that the keyspace "test" was unaffected
        assertEquals(1, (int) db.tx(commit1).get("test", "x"));
        assertEquals(2, (int) db.tx(commit2).get("test", "x"));
        assertEquals(3, (int) db.tx(commit3).get("test", "x"));
        assertEquals(4, (int) db.tx(commit4).get("test", "x"));
        assertEquals(5, (int) db.tx(commit5).get("test", "x"));

        // check that the commits 2 and 3 are indeed gone
        assertEquals("a1", db.tx(commit1).get("mykeyspace", "a"));
        assertEquals("a1", db.tx(commit2).get("mykeyspace", "a"));
        assertEquals("a1", db.tx(commit3).get("mykeyspace", "a"));
        assertEquals("a1", db.tx(commit4).get("mykeyspace", "a"));
        assertNull(db.tx(commit5).get("mykeyspace", "a"));
        // only insertion and deletion of a should remain
        assertThat(Iterators.size(db.tx().history("mykeyspace", "a")), is(2));

        // b should not have been affected
        assertEquals("b1", db.tx(commit1).get("mykeyspace", "b"));
        assertEquals("b1", db.tx(commit2).get("mykeyspace", "b"));
        assertEquals("b1", db.tx(commit3).get("mykeyspace", "b"));
        assertEquals("b1", db.tx(commit4).get("mykeyspace", "b"));
        assertEquals("b1", db.tx(commit5).get("mykeyspace", "b"));
        assertThat(Iterators.size(db.tx().history("mykeyspace", "b")), is(1));

        // the deletion of c should be gone as well
        assertEquals("c1", db.tx(commit1).get("mykeyspace", "c"));
        assertEquals("c1", db.tx(commit2).get("mykeyspace", "c"));
        assertEquals("c1", db.tx(commit3).get("mykeyspace", "c"));
        assertEquals("c1", db.tx(commit4).get("mykeyspace", "c"));
        assertEquals("c1", db.tx(commit5).get("mykeyspace", "c"));
        assertThat(Iterators.size(db.tx().history("mykeyspace", "c")), is(1));

        // d should first appear on commit 5
        assertNull(db.tx(commit1).get("mykeyspace", "d"));
        assertNull(db.tx(commit2).get("mykeyspace", "d"));
        assertNull(db.tx(commit3).get("mykeyspace", "d"));
        assertNull(db.tx(commit4).get("mykeyspace", "d"));
        assertEquals("d4", db.tx(commit5).get("mykeyspace", "d"));
        assertThat(Iterators.size(db.tx().history("mykeyspace", "d")), is(1));
    }


    /**
     * This method creates a very basic test data setup.
     *
     * <p>
     * The setup contains two keys ("A" and "B") which are inserted in the
     * {@linkplain ChronoDBConstants#DEFAULT_KEYSPACE_NAME default} keyspace on the
     * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch.
     *
     * <p>
     * Here are the values which are associated with the keys, as well as the rollovers which occur:
     * <ul>
     * <li>A = 1, B = 1
     * <li>A = 2, B = 2
     * <li><i>rollover</i>
     * <li>A = 3, B = 3,
     * <li>A = 4, B = 4
     * <li><i>rollover</i>
     * <li>A = 5, B = 5
     * <li>A = 6, B = 6
     * <li><i>rollover</i>
     * </ul>
     *
     * @param db The database to insert the given dataset into. Must not be <code>null</code>. Is assumed to be an
     *           <i>empty</i> database.
     * @return The list of the 4 timestamps which mark the starting timestamps of each chunk (the first one is always
     * zero).
     */
    private static List<Long> createFourRolloverModel(final ChronoDB db) {
        List<Long> timestamps = Lists.newArrayList();
        timestamps.add(db.tx().getTimestamp());
        // first chunk
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("A", 1);
            tx.put("B", 1);
            tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("A", 2);
            tx.put("B", 2);
            tx.commit();
        }
        sleep(2);
        db.getMaintenanceManager().performRolloverOnMaster();
        sleep(2);

        timestamps.add(db.tx().getTimestamp());
        // second chunk
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("A", 3);
            tx.put("B", 3);
            tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("A", 4);
            tx.put("B", 4);
            tx.commit();
        }
        sleep(2);
        db.getMaintenanceManager().performRolloverOnMaster();
        sleep(2);

        timestamps.add(db.tx().getTimestamp());
        // third chunk
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("A", 5);
            tx.put("B", 5);
            tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("A", 6);
            tx.put("B", 6);
            tx.commit();
        }
        sleep(2);
        db.getMaintenanceManager().performRolloverOnMaster();
        timestamps.add(db.tx().getTimestamp());
        return timestamps;
    }

}
