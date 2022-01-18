package org.chronos.chronodb.test.cases.engine.dateback;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.BranchManager;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Dateback;
import org.chronos.chronodb.api.DatebackManager;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.dateback.log.DatebackOperation;
import org.chronos.chronodb.internal.api.dateback.log.IPurgeKeyspaceOperation;
import org.chronos.chronodb.internal.impl.dateback.log.InjectEntriesOperation;
import org.chronos.chronodb.internal.impl.dateback.log.PurgeCommitsOperation;
import org.chronos.chronodb.internal.impl.dateback.log.PurgeEntryOperation;
import org.chronos.chronodb.internal.impl.dateback.log.PurgeKeyOperation;
import org.chronos.chronodb.internal.impl.dateback.log.v2.TransformCommitOperation2;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class BasicDatebackTest extends AllChronoDBBackendsTest {

    @Test
    public void canAccessDatebackAPI() {
        ChronoDB db = this.getChronoDB();
        assertNotNull(db.getDatebackManager());
    }

    @Test
    public void canStartDatebackProcess() {
        ChronoDB db = this.getChronoDB();
        AtomicBoolean enteredDateback = new AtomicBoolean(false);
        db.getDatebackManager().datebackOnMaster(dateback -> {
            assertNotNull(dateback);
            enteredDateback.set(true);
        });
        assertTrue(enteredDateback.get());
    }

    @Test
    public void datebackIsClosedAfterEvaluatingPassedFunction() {
        ChronoDB db = this.getChronoDB();
        // try to "steal" the dateback object
        Set<Dateback> datebacks = Sets.newHashSet();
        db.getDatebackManager().datebackOnMaster(dateback -> {
            datebacks.add(dateback);
        });
        assertEquals(1, datebacks.size());
        // calling ANY method on this dateback should throw an exception
        Dateback dateback = Iterables.getOnlyElement(datebacks);
        assertTrue(dateback.isClosed());
        try {
            dateback.inject("default", "test", 123, "Hello world!");
            fail("Managed to steal a dateback object and use it arbitrarily outside of the process.");
        } catch (IllegalStateException expected) {
            // pass
        }
    }

    @Test
    public void cannotOpenTransactionsWhileDatebackProcessIsOngoing() {
        ChronoDB db = this.getChronoDB();
        db.getDatebackManager().datebackOnMaster(dateback -> {
            try {
                db.tx();
                fail("Managed to open a transaction while in dateback!");
            } catch (IllegalStateException expected) {
                // pass
            }
        });
    }

    @Test
    public void canQueryDatabaseWithDatebackObject() {
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx = db.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        long afterFirstCommit = tx.commit();
        tx.put("Number", 42);
        tx.remove("Foo");
        long afterSecondCommit = tx.commit();
        db.getDatebackManager().datebackOnMaster(dateback -> {
            // we should be able to query HEAD at the moment
            assertThat(dateback.get(afterSecondCommit, ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Number"), is(42));
            assertThat(dateback.get(afterSecondCommit, ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Foo"), is(nullValue()));

            // now we alter our last commit
            dateback.transformCommit(afterSecondCommit, originalCommit -> {
                HashMap<QualifiedKey, Object> newCommit = Maps.newHashMap();
                newCommit.put(QualifiedKey.createInDefaultKeyspace("Number"), Dateback.UNCHANGED);
                newCommit.put(QualifiedKey.createInDefaultKeyspace("Baz"), "Boom");
                return newCommit;
            });

            // queries on afterFirstCommit should still be okay
            assertThat(dateback.get(afterFirstCommit, ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Hello"), is("World"));
            assertThat(dateback.get(afterFirstCommit, ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Foo"), is("Bar"));

            // ... but queries after that should throw an exception
            try {
                dateback.get(afterSecondCommit, ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Number");
                fail("Managed to perform a get() on a potentially dirty timestamp while a dateback was running");
            } catch (IllegalArgumentException expected) {
                // pass
            }
        });
        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(TransformCommitOperation2.class, op -> {
            assertThat(op.getCommitTimestamp(), is(afterSecondCommit));
        });
    }


    @Test
    public void canPurgeSingleEntry() {
        ChronoDB db = this.getChronoDB();
        { // insert some test data
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit();
        }
        long timestamp = db.tx().getTimestamp();

        // purge an entry
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.purgeEntry(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Hello", timestamp);
        });

        // assert that it's gone
        assertEquals(Collections.singleton("Foo"), db.tx().keySet());
        assertNull(db.tx().get("Hello"));
        // assert that the other entry is left untouched
        assertEquals("Bar", db.tx().get("Foo"));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(PurgeEntryOperation.class, op -> {
            assertThat(op.getOperationTimestamp(), is(timestamp));
            assertThat(op.getKeyspace(), is(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
            assertThat(op.getKey(), is("Hello"));
        });
    }

    @Test
    public void timestampNeedsToBePreciseWhenPurgingEntries() {
        ChronoDB db = this.getChronoDB();
        { // insert some test data
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit();
        }
        long timestamp = db.tx().getTimestamp();

        // purge an entry
        db.getDatebackManager().datebackOnMaster(dateback -> {
            // note that the timestamp is "off by one" here
            boolean purged = dateback.purgeEntry(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Hello", timestamp + 1);
            assertFalse(purged);
        });

        // make sure the operation was not logged (because it wasn't successful / didn't change anything)
        List<DatebackOperation> allOperations = db.getDatebackManager().getAllPerformedDatebackOperations();
        assertThat(allOperations.size(), is(0));
    }

    @Test
    public void canPurgeKey() {
        ChronoDB db = this.getChronoDB();
        { // test data #1
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit();
        }
        long afterFirstCommit = db.tx().getTimestamp();

        { // test data #2
            ChronoDBTransaction tx = db.tx();
            tx.put("Foo", "Baz");
            tx.commit();
        }
        long afterSecondCommit = db.tx().getTimestamp();

        { // test data #3
            ChronoDBTransaction tx = db.tx();
            tx.remove("Foo");
            tx.commit();
        }
        long afterThirdCommit = db.tx().getTimestamp();

        // purge the "Foo" key
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.purgeKey(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Foo");
        });

        // make sure that it is gone
        assertNull(db.tx(afterThirdCommit).get("Foo"));
        assertNull(db.tx(afterSecondCommit).get("Foo"));
        assertNull(db.tx(afterFirstCommit).get("Foo"));
        Iterator<Long> history = db.tx().history("Foo");
        assertFalse(history.hasNext());

        // make sure that the other key still exists
        assertEquals("World", db.tx().get("Hello"));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(PurgeKeyOperation.class, op -> {
            assertThat(op.getFromTimestamp(), is(greaterThan(0L)));
            assertThat(op.getFromTimestamp(), is(lessThan(op.getToTimestamp())));
            assertThat(op.getKeyspace(), is(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
            assertThat(op.getKey(), is("Foo"));
        });
    }

    @Test
    public void canPurgeKeyDeletionWithPredicateOnNullValue() {
        ChronoDB db = this.getChronoDB();
        { // test data #1
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit();
        }
        { // test data #2
            ChronoDBTransaction tx = db.tx();
            tx.remove("Foo");
            tx.commit();
        }
        { // test data #3
            ChronoDBTransaction tx = db.tx();
            tx.remove("Hello");
            tx.commit();
        }
        assertEquals(Collections.emptySet(), db.tx().keySet());

        // purge all deletions on "Hello"
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.purgeKey(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Hello", (time, value) -> value == null);
        });

        // make sure that "Hello" exists now (the deletion entry was purged)
        assertEquals(Collections.singleton("Hello"), db.tx().keySet());
        assertEquals("World", db.tx().get("Hello"));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(PurgeKeyOperation.class, op -> {
            assertThat(op.getFromTimestamp(), is(greaterThan(0L)));
            assertThat(op.getFromTimestamp(), is(lessThanOrEqualTo(op.getToTimestamp())));
            assertThat(op.getKeyspace(), is(ChronoDBConstants.DEFAULT_KEYSPACE_NAME));
            assertThat(op.getKey(), is("Hello"));
        });
    }

    @Test
    public void canPurgeSingleCommit() {
        ChronoDB db = this.getChronoDB();
        { // test data #1
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit("First");
        }
        { // test data #2
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "Chronos");
            tx.remove("Foo");
            tx.commit("Second");
        }
        long secondCommitTimestamp = db.tx().getTimestamp();
        { // test data #2
            ChronoDBTransaction tx = db.tx();
            tx.put("John", "Doe");
            tx.commit("Third");
        }
        assertEquals("Chronos", db.tx().get("Hello"));
        assertNull(db.tx().get("Foo"));
        assertEquals("Doe", db.tx().get("John"));

        // purge the second commit
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.purgeCommit(secondCommitTimestamp);
        });

        // "Hello" should point to "World" again, and "Foo" should not be deleted and point to "Bar"
        assertEquals("World", db.tx().get("Hello"));
        assertEquals("Bar", db.tx().get("Foo"));
        assertEquals("Doe", db.tx().get("John"));

        // also, we should not have this commit in the commit log anymore
        assertNull(db.tx().getCommitMetadata(secondCommitTimestamp));

        // the keys should not have it in their history anymore either
        assertEquals(1, Iterators.size(db.tx().history("Hello")));
        assertEquals(1, Iterators.size(db.tx().history("Foo")));
        assertEquals(1, Iterators.size(db.tx().history("John")));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(PurgeCommitsOperation.class, op -> {
            assertThat(op.getCommitTimestamps(), contains(secondCommitTimestamp));
        });
    }

    @Test
    public void canPurgeMultipleCommits() {
        ChronoDB db = this.getChronoDB();
        { // test data #1
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit("First");
        }
        { // test data #2
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "Chronos");
            tx.remove("Foo");
            tx.commit("Second");
        }
        long secondCommitTimestamp = db.tx().getTimestamp();
        { // test data #2
            ChronoDBTransaction tx = db.tx();
            tx.put("John", "Doe");
            tx.commit("Third");
        }
        long thirdCommitTimestamp = db.tx().getTimestamp();
        assertEquals("Chronos", db.tx().get("Hello"));
        assertNull(db.tx().get("Foo"));
        assertEquals("Doe", db.tx().get("John"));

        System.out.println("Second commit timestamp: " + secondCommitTimestamp);
        System.out.println("Third commit timestamp:  " + thirdCommitTimestamp);

        assertThat(thirdCommitTimestamp, is(greaterThan(secondCommitTimestamp)));

        // purge the last two commits
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.purgeCommits(secondCommitTimestamp, thirdCommitTimestamp);
        });

        // "Hello" should point to "World" again, "Foo should point to "Bar", and "John" should not exist
        assertEquals("World", db.tx().get("Hello"));
        assertEquals("Bar", db.tx().get("Foo"));
        assertNull(db.tx().get("John"));

        assertEquals(1, Iterators.size(db.tx().getCommitTimestampsBetween(0, thirdCommitTimestamp)));

        // the keys should not have it in their history anymore either
        assertEquals(1, Iterators.size(db.tx().history("Hello")));
        assertEquals(1, Iterators.size(db.tx().history("Foo")));
        assertEquals(0, Iterators.size(db.tx().history("John")));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(PurgeCommitsOperation.class, op -> {
            assertThat(op.getCommitTimestamps(), containsInAnyOrder(secondCommitTimestamp, thirdCommitTimestamp));
        });
    }

    @Test
    public void canInjectSingleEntryInThePast() {
        ChronoDB db = this.getChronoDB();
        { // test data
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Baz");
            tx.commit();
        }
        long now = db.tx().getTimestamp();

        // inject the value
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Foo", 1234L, "Bar");
        });

        // make sure that the injected value is present in the history
        assertEquals(Sets.newHashSet(1234L, now), Sets.newHashSet(db.tx().history("Foo")));
        // we should be able to retrieve this value normally
        assertEquals("Bar", db.tx(1234L).get("Foo"));
        // we should also see the new commit
        assertEquals(Collections.singletonList(1234L), db.tx().getCommitTimestampsBefore(1235L, 1));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(InjectEntriesOperation.class, op -> {
            assertThat(op.getOperationTimestamp(), is(1234L));
            assertThat(op.getInjectedKeys(), hasItems(QualifiedKey.createInDefaultKeyspace("Foo")));
        });
    }

    @Test
    public void cannotInjectIntoTheFuture() {
        ChronoDB db = this.getChronoDB();
        long timestamp = System.currentTimeMillis() + 100000;
        db.getDatebackManager().datebackOnMaster(dateback -> {
            try {
                dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Hello", timestamp, "World");
                fail("Managed to inject entries into the future!");
            } catch (IllegalArgumentException expected) {
                // pass
            }
        });

        // make sure the operation was not logged
        List<DatebackOperation> allOperations = db.getDatebackManager().getAllPerformedDatebackOperations();
        assertThat(allOperations.size(), is(0));
    }

    @Test
    public void canInjectSingleEntryOnAnUnusedKey() {
        ChronoDB db = this.getChronoDB();
        long timestamp = System.currentTimeMillis();
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Hello", timestamp, "World");
        });
        // make sure that the NOW timestamp was advanced
        assertEquals(timestamp, db.tx().getTimestamp());
        // make sure that the entry is now in the database
        assertEquals("World", db.tx().get("Hello"));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(InjectEntriesOperation.class, op -> {
            assertThat(op.getInjectedKeys(), hasItems(QualifiedKey.createInDefaultKeyspace("Hello")));
        });
    }


    @Test
    public void canInjectDeletion() {
        ChronoDB db = this.getChronoDB();
        { // insert test data
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit();
        }

        // make sure that at least 1 millisecond has passed between the commit and our dateback,
        // otherwise we would override our entry with a deletion (which is not what we want)
        sleep(1);

        // inject the deletion
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Foo", System.currentTimeMillis(), null);
        });

        // make sure that "Foo" is gone in the head revision
        assertEquals(Collections.singleton("Hello"), db.tx().keySet());
        // "Foo" should have 2 history entries (creation + deletion)
        assertEquals(2, Iterators.size(db.tx().history("Foo")));
        // our deletion injection should have created a commit
        assertEquals(2, db.tx().getCommitTimestampsAfter(0, 10).size());

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(InjectEntriesOperation.class, op -> {
            assertThat(op.getOperationTimestamp(), is(db.tx().getTimestamp()));
            assertThat(op.getInjectedKeys(), containsInAnyOrder(QualifiedKey.createInDefaultKeyspace("Foo")));
        });
    }

    @Test
    public void canOverrideEntryWithInjection() {
        ChronoDB db = this.getChronoDB();
        { // insert test data
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit();
        }
        long now = db.tx().getTimestamp();

        // inject an entry exactly at the same coordinates
        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Foo", now, "Baz");
        });

        // assert that "Foo" has the proper new value
        assertEquals("Baz", db.tx().get("Foo"));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(InjectEntriesOperation.class, op -> {
            assertThat(op.getOperationTimestamp(), is(now));
            assertThat(op.getInjectedKeys(), contains(QualifiedKey.createInDefaultKeyspace("Foo")));
        });
    }

    @Test
    public void canInjectMultipleEntries() {
        ChronoDB db = this.getChronoDB();
        { // insert test data
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit();
        }
        long latestTimestampBeforeInjection = db.tx().getTimestamp();

        // make sure that at least 1 millisecond has passed between the commit and our dateback,
        // otherwise we would override our entry with a deletion (which is not what we want)
        sleep(1);

        // inject new values
        db.getDatebackManager().datebackOnMaster(dateback -> {
            String keyspace = ChronoDBConstants.DEFAULT_KEYSPACE_NAME;
            Map<QualifiedKey, Object> entries = Maps.newHashMap();
            entries.put(QualifiedKey.create(keyspace, "Foo"), "Baz");
            entries.put(QualifiedKey.create(keyspace, "Hello"), null); // null for deletion
            entries.put(QualifiedKey.create(keyspace, "John"), "Doe");
            dateback.inject(System.currentTimeMillis(), entries, "Injected Commit");
        });

        long now = db.tx().getTimestamp();
        // this injection should have advanced our timestamp
        assertTrue(now > latestTimestampBeforeInjection);
        // the new commit should appear in the log
        assertEquals("Injected Commit", db.tx().getCommitMetadata(now));
        // check that the key set is correct
        assertEquals(Sets.newHashSet("Foo", "John"), db.tx().keySet());
        // check that the assigned values are correct
        assertEquals("Baz", db.tx().get("Foo"));
        assertEquals("Doe", db.tx().get("John"));
        // assert that the history lengths are correct
        assertEquals(2, Iterators.size(db.tx().history("Hello")));
        assertEquals(2, Iterators.size(db.tx().history("Foo")));
        assertEquals(1, Iterators.size(db.tx().history("John")));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(InjectEntriesOperation.class, op -> {
            assertThat(op.getInjectedKeys(), containsInAnyOrder(
                QualifiedKey.createInDefaultKeyspace("Foo"),
                QualifiedKey.createInDefaultKeyspace("Hello"),
                QualifiedKey.createInDefaultKeyspace("John")
            ));
        });
    }

    @Test
    public void cannotInjectEntriesBeforeBranchingTimestamp() {
        ChronoDB db = this.getChronoDB();
        {// insert test data on MASTER
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Foo", "Bar");
            tx.commit();
        }
        // create the test branch
        Branch testBranch = db.getBranchManager().createBranch("test");

        // start a dateback on the test branch and attempt to inject values before the branching timestamp
        // (which would make them entries of "master", which is clearly not intended)
        db.getDatebackManager().dateback(testBranch.getName(), dateback -> {
            try {
                dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "John", testBranch.getBranchingTimestamp() - 1,
                    "Doe");
                fail("Managed to inject data on branch before the branching timestamp!");
            } catch (IllegalArgumentException expected) {
                // pass
            }
        });

        assertThat(db.getDatebackManager().getAllPerformedDatebackOperations(), is(empty()));
    }

    @Test
    public void canInjectEntryIntoNewKeyspace() {
        ChronoDB db = this.getChronoDB();

        db.getDatebackManager().datebackOnMaster(dateback -> {
            dateback.inject("people", "John", 1234L, "Doe");
        });

        assertEquals(Sets.newHashSet(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "people"), db.tx().keyspaces());
        assertEquals("Doe", db.tx().get("people", "John"));

        // make sure the operation was logged
        this.assertSingleDatebackOperationWasLogged(InjectEntriesOperation.class, op -> {
            assertThat(op.getOperationTimestamp(), is(1234L));
            assertThat(op.getInjectedKeys(), contains(QualifiedKey.create("people", "John")));
        });
    }

    @Test
    public void testDatebackOnBranches() {
        ChronoDB db = this.getChronoDB();
        long commit1;
        long commit2;
        long commit3;
        long commit4;
        long commit5;
        long commit6;
        long commit7;
        long commit8;
        long commit9;
        long commit10;

        { // first commit on master
            ChronoDBTransaction tx = db.tx();
            tx.put("John", "Doe");
            commit1 = tx.commit();
        }
        { // second commit on master
            ChronoDBTransaction tx = db.tx();
            tx.put("Jane", "Doe");
            commit2 = tx.commit();
        }
        BranchManager branchManager = db.getBranchManager();
        branchManager.createBranch("B1");
        {
            ChronoDBTransaction tx = db.tx("B1");
            tx.put("John", "Foo");
            commit3 = tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx();
            tx.remove("John");
            commit4 = tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("John", "HereAgain");
            commit5 = tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx("B1");
            tx.put("Isaac", "Newton");
            commit6 = tx.commit();
        }
        branchManager.createBranch("B1", "B2");
        {
            ChronoDBTransaction tx = db.tx("B2");
            tx.put("John", "Newton");
            commit7 = tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx("B1");
            tx.remove("Jane");
            commit8 = tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx("B1");
            tx.put("Mao", "Almaz");
            commit9 = tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx();
            tx.put("Isaac", "Einstein");
            commit10 = tx.commit();
        }

        DatebackManager datebackManager = db.getDatebackManager();
        datebackManager.datebackOnMaster(dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Alien", commit1, "E.T.");
        });
        assertThat(db.tx("B2").get("Alien"), is("E.T."));

        datebackManager.datebackOnMaster(dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Jane", commit4, null);
        });
        assertThat(db.tx().get("Jane"), is(nullValue()));
        assertThat(db.tx("B2").get("Jane"), is("Doe"));

        datebackManager.dateback("B1", dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Foo", commit3, "Bar");
        });
        assertThat(db.tx().get("Foo"), is(nullValue()));
        assertThat(db.tx("B1").get("Foo"), is("Bar"));
        assertThat(db.tx("B2").get("Foo"), is("Bar"));

        datebackManager.dateback("B1", dateback -> {
            dateback.purgeEntry(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Jane", commit8);
        });
        assertThat(db.tx("B1").get("Jane"), is("Doe"));

        datebackManager.dateback("B2", dateback -> {
            dateback.inject(ChronoDBConstants.DEFAULT_KEYSPACE_NAME, "Jane", commit7, "Newton");
        });
        assertThat(db.tx("B2").get("Jane"), is("Newton"));
        assertThat(db.tx("B1").get("Jane"), is("Doe"));
        assertThat(db.tx().get("Jane"), is(nullValue()));

        List<DatebackOperation> b2Dateback = datebackManager.getDatebackOperationsAffectingTimestamp("B2", commit7);
        assertThat(b2Dateback.size(), is(3));
        List<DatebackOperation> b1Dateback = datebackManager.getDatebackOperationsAffectingTimestamp("B1", commit9);
        assertThat(b1Dateback.size(), is(3));
        List<DatebackOperation> dateback = datebackManager.getDatebackOperationsAffectingTimestamp(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, commit10);
        assertThat(dateback.size(), is(2));

    }

    @Test
    public void canPurgeKeyspace() {
        ChronoDB db = this.getChronoDB();
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
        { // commit 4
            ChronoDBTransaction tx = db.tx();
            tx.put("mykeyspace", "a", "a4");
            tx.put("mykeyspace", "d", "d3");
            tx.put("test", "x", 4);
            commit4 = tx.commit();
        }
        {
            ChronoDBTransaction tx = db.tx();
            tx.remove("mykeyspace", "a");
            tx.put("mykeyspace", "d", "d4");
            tx.put("test", "x", 5);
            commit5 = tx.commit();
        }

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

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    @SuppressWarnings("unchecked")
    private <T extends DatebackOperation> void assertSingleDatebackOperationWasLogged(Class<T> type, Consumer<T> check) {
        ChronoDB db = this.getChronoDB();
        List<DatebackOperation> allOperations = db.getDatebackManager().getAllPerformedDatebackOperations();
        assertThat(allOperations.size(), is(1));
        DatebackOperation operation = Iterables.getOnlyElement(allOperations);
        assertThat(operation, is(instanceOf(type)));
        assertThat(operation.getBranch(), is(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER));
        assertThat(operation.getWallClockTime(), is(greaterThanOrEqualTo(db.tx().getTimestamp())));
        T op = (T) operation;
        check.accept(op);
        List<DatebackOperation> operationsOnMaster = db.getDatebackManager().getDatebackOperationsAffectingTimestamp(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, System.currentTimeMillis());
        assertThat(operationsOnMaster, is(allOperations));
        List<DatebackOperation> operationsInRange = db.getDatebackManager().getDatebackOperationsPerformedBetween(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, 0, System.currentTimeMillis());
        assertThat(operationsInRange, is(allOperations));
    }
}
