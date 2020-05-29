package org.chronos.chronodb.test.cases.engine.reopening;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.AllBackendsTest.DontRunWithBackend;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronodb.test.cases.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
// these tests do not make sense with non-persistent backends.
@DontRunWithBackend({InMemoryChronoDB.BACKEND_NAME})
public class ChronoDBReopeningTest extends AllChronoDBBackendsTest {

    @Test
    public void reopeningChronoDbWorks() {
        ChronoDB db = this.getChronoDB();
        assertNotNull(db);
        ChronoDB db2 = this.closeAndReopenDB();
        assertNotNull(db2);
        assertTrue(db != db2);
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "10000")
    public void reopeningChronoDbPreservesConfiguration() {
        ChronoDB db = this.getChronoDB();
        assertNotNull(db);
        // assert that the settings from the test annotations were applied correctly
        assertTrue(db.getConfiguration().isCachingEnabled());
        assertEquals(10000L, (long) db.getConfiguration().getCacheMaxSize());
        // reinstantiate the DB
        ChronoDB db2 = this.closeAndReopenDB();
        assertNotNull(db2);
        // assert that the settings have carried over
        assertTrue(db2.getConfiguration().isCachingEnabled());
        assertEquals(10000L, (long) db.getConfiguration().getCacheMaxSize());
    }

    @Test
    public void reopeningChronoDbPreservesContents() {
        ChronoDB db = this.getChronoDB();
        assertNotNull(db);
        // fill the db with some data
        ChronoDBTransaction tx = db.tx();
        tx.put("k1", "Hello World");
        tx.put("k2", "Foo Bar");
        tx.put("k3", 42);
        tx.commit();
        // reinstantiate
        ChronoLogger.log("Reinstantiating DB");
        ChronoDB db2 = this.closeAndReopenDB();
        // check that the content is still there
        ChronoDBTransaction tx2 = db2.tx();
        Set<String> keySet = tx2.keySet();
        assertEquals(3, keySet.size());
        assertTrue(keySet.contains("k1"));
        assertTrue(keySet.contains("k2"));
        assertTrue(keySet.contains("k3"));
        assertEquals("Hello World", tx2.get("k1"));
        assertEquals("Foo Bar", tx2.get("k2"));
        assertEquals(42, (int) tx2.get("k3"));
    }

    @Test
    public void reopeningChronoDbPreservesIndexers() {
        ChronoDB db = this.getChronoDB();
        assertNotNull(db);
        // add some indexers
        db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
        db.getIndexManager().addIndexer("test", new NamedPayloadNameIndexer());
        // assert that the indices are present
        assertTrue(db.getIndexManager().getIndexNames().contains("name"));
        assertTrue(db.getIndexManager().getIndexNames().contains("test"));
        assertEquals(2, db.getIndexManager().getIndexNames().size());

        // reinstantiate the DB
        ChronoDB db2 = this.closeAndReopenDB();
        // assert that the indices are still present
        assertTrue(db2.getIndexManager().getIndexNames().contains("name"));
        assertTrue(db2.getIndexManager().getIndexNames().contains("test"));
        assertEquals(2, db2.getIndexManager().getIndexNames().size());
    }

    @Test
    public void reopeningChronoDbPreservesIndexDirtyFlags() {
        ChronoDB db = this.getChronoDB();
        assertNotNull(db);
        // add some indexers
        db.getIndexManager().addIndexer("name", new NamedPayloadNameIndexer());
        db.getIndexManager().reindexAll();
        db.getIndexManager().addIndexer("test", new NamedPayloadNameIndexer());
        // make sure that 'name' isn't dirty
        assertFalse(db.getIndexManager().getDirtyIndices().contains("name"));
        // ... but the 'test' index should still be dirty
        assertTrue(db.getIndexManager().getDirtyIndices().contains("test"));

        // reopen the db
        ChronoDB db2 = this.closeAndReopenDB();

        // assert that the 'name' index is not dirty, but the 'test' index is
        assertFalse(db2.getIndexManager().getDirtyIndices().contains("name"));
        assertTrue(db2.getIndexManager().getDirtyIndices().contains("test"));
    }

    @Test
    public void reopeningChronoDbPreservesBranches() {
        ChronoDB db = this.getChronoDB();
        assertNotNull(db);
        { // insert data into "master"
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.commit();
        }
        db.getBranchManager().createBranch("Test");
        { // insert data into "Test"
            ChronoDBTransaction tx = db.tx("Test");
            tx.put("Foo", "Bar");
            tx.commit();
        }
        db.getBranchManager().createBranch("Test", "TestSub");
        { // insert data into "TestSub"
            ChronoDBTransaction tx = db.tx("TestSub");
            tx.put("Foo", "Baz");
            tx.commit();
        }
        // assert that the data is present
        assertEquals("World", db.tx().get("Hello"));
        assertEquals("Bar", db.tx("Test").get("Foo"));
        assertEquals("Baz", db.tx("TestSub").get("Foo"));

        // close and reopen the db
        ChronoDB db2 = this.closeAndReopenDB();

        // assert that the data is still present
        assertEquals("World", db2.tx().get("Hello"));
        assertEquals("Bar", db2.tx("Test").get("Foo"));
        assertEquals("Baz", db2.tx("TestSub").get("Foo"));
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "200000")
    public void mosaicCacheWorksWithReopenedRolloverDB(){
        ChronoDB db = this.getChronoDB();
        this.assumeRolloverIsSupported(db);
        this.assumeIsPersistent(db);

        { // initial commit
            ChronoDBTransaction tx = db.tx();
            tx.put("Hello", "World");
            tx.put("Value", 1);
            tx.commit();
        }

        { // update...
            ChronoDBTransaction tx = db.tx();
            tx.put("Value", 2);
            tx.commit();
        }

        long beforeRollover = db.getBranchManager().getMasterBranch().getNow();

        // rollover
        db.getMaintenanceManager().performRolloverOnMaster();

        long afterRollover = db.getBranchManager().getMasterBranch().getNow();
        assertThat(beforeRollover, is(lessThan(afterRollover)));

        { // perform another update
            ChronoDBTransaction tx = db.tx();
            tx.put("Value", 3);
            tx.commit();
        }

        // restart
        db = this.closeAndReopenDB();

        { // query the old state (this will fill the cache with an entry from chunk 0)
            ChronoDBTransaction tx = db.tx(beforeRollover);
            Object value = tx.get("Value");
            assertThat(value, is(2));
        }

        { // query again at head (which is on chunk 1)
            ChronoDBTransaction tx = db.tx();
            // we should see the HEAD value
            assertThat(tx.get("Value"), is(3));
        }

    }
}
