package org.chronos.chronodb.test.cases.engine.transaction;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.SerializationManager;
import org.chronos.chronodb.api.exceptions.ChronoDBTransactionException;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class BasicTransactionTest extends AllChronoDBBackendsTest {

    @Test
    public void basicCommitAndGetWorks() {
        ChronoDB chronoDB = this.getChronoDB();
        ChronoDBTransaction tx = chronoDB.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        tx.commit();
        assertEquals("World", tx.get("Hello"));
        assertEquals("Bar", tx.get("Foo"));
    }

    @Test
    public void cantOpenTransactionIntoTheFuture() {
        ChronoDB chronoDB = this.getChronoDB();
        ChronoDBTransaction tx = chronoDB.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        tx.commit();
        try {
            tx = chronoDB.tx(System.currentTimeMillis() + 1000);
            fail("Managed to open a transaction into the future!");
        } catch (ChronoDBTransactionException e) {
            // expected
        }
    }

    @Test
    public void canRemoveKeys() {
        ChronoDB chronoDB = this.getChronoDB();
        ChronoDBTransaction tx = chronoDB.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        tx.commit();
        tx.remove("Hello");
        tx.commit();

        assertEquals(false, tx.exists("Hello"));
        assertEquals(2, Iterators.size(tx.history("Hello")));
    }

    @Test
    public void removalOfKeysAffectsKeySet() {
        ChronoDB chronoDB = this.getChronoDB();
        ChronoDBTransaction tx = chronoDB.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        tx.commit();
        tx.remove("Hello");
        tx.commit();

        assertFalse(tx.exists("Hello"));
        assertEquals(1, tx.keySet().size());
    }

    @Test
    public void removedKeysNoLongerExist() {
        ChronoDB chronoDB = this.getChronoDB();
        ChronoDBTransaction tx = chronoDB.tx();
        tx.put("Hello", "World");
        tx.put("Foo", "Bar");
        tx.commit();
        tx.remove("Hello");
        tx.commit();

        assertTrue(tx.exists("Foo"));
        assertFalse(tx.exists("Hello"));
    }

    @Test
    public void transactionsAreIsolated() {
        ChronoDB chronoDB = this.getChronoDB();

        ChronoDBTransaction tx = chronoDB.tx();
        tx.put("Value1", 1234);
        tx.put("Value2", 1000);
        tx.commit();

        // Transaction A: read-only; used to check isolation level
        ChronoDBTransaction txA = chronoDB.tx();

        // Transaction B: will set Value1 to 47
        ChronoDBTransaction txB = chronoDB.tx();

        // Transaction C: will set Value2 to 2000
        ChronoDBTransaction txC = chronoDB.tx();

        // perform the work in C (while B is open)
        txC.put("Value2", 2000);
        txC.commit();

        // make sure that isolation level of Transaction A is not violated
        assertEquals(1234, (int) txA.get("Value1"));
        assertEquals(1000, (int) txA.get("Value2"));

        // perform work in B (note that we change different keys than in C)
        txB.put("Value1", 47);
        txB.commit();

        // make sure that isolation level of Transaction A is not violated
        assertEquals(1234, (int) txA.get("Value1"));
        assertEquals(1000, (int) txA.get("Value2"));

        // Transaction D: read-only; used to check that commits were successful
        ChronoDBTransaction txD = chronoDB.tx();

        // ensure that D sees the results of B and C
        assertEquals(47, (int) txD.get("Value1"));
        assertEquals(2000, (int) txD.get("Value2"));
    }

    @Test
    public void canRemoveAndPutInSameTransaction() {
        ChronoDB chronoDB = this.getChronoDB();

        ChronoDBTransaction tx1 = chronoDB.tx();
        tx1.put("Hello", "World");
        tx1.put("programming", "Foo", "Bar");
        tx1.commit();

        ChronoDBTransaction tx2 = chronoDB.tx();
        tx2.remove("Hello");
        tx2.remove("programming", "Foo");
        tx2.put("Hello", "ChronoDB");
        tx2.put("programming", "Foo", "Baz");
        tx2.commit();

        ChronoDBTransaction tx3 = chronoDB.tx();
        assertEquals("ChronoDB", tx3.get("Hello"));
        assertEquals("Baz", tx3.get("programming", "Foo"));
    }

    @Test
    public void commitReturnsTheCommitTimestamp() {
        ChronoDB db = this.getChronoDB();

        ChronoDBTransaction tx1 = db.tx();
        tx1.put("Hello", "World");
        long time1 = tx1.commit();

        assertEquals(db.tx().getTimestamp(), time1);

        ChronoDBTransaction tx2 = db.tx();
        tx2.put("Foo", "Bar");
        long time2 = tx2.commit();

        assertEquals(db.tx().getTimestamp(), time2);

        ChronoDBTransaction tx3 = db.tx();
        tx3.put("Foo", "Baz");
        long time3 = tx3.commit();

        assertEquals(db.tx().getTimestamp(), time3);

        assertTrue(time1 >= 0);
        assertTrue(time2 >= 0);
        assertTrue(time3 >= 0);

        assertTrue(time2 > time1);
        assertTrue(time3 > time2);

        ChronoDBTransaction tx = db.tx();
        List<Long> timestamps = tx.getCommitTimestampsAfter(0, 3);
        assertEquals(3, timestamps.size());
        assertTrue(timestamps.contains(time1));
        assertTrue(timestamps.contains(time2));
        assertTrue(timestamps.contains(time3));
    }

    @Test
    public void canPerformGetBinary(){
        ChronoDB db = this.getChronoDB();

        { // first commit
            ChronoDBTransaction tx = db.tx();
            tx.put("hello", "world");
            tx.put("number", 42);
            tx.commit();
        }

        { // second commit
            ChronoDBTransaction tx = db.tx();
            tx.put("hello", "chronos");
            tx.put("foo", "bar");
            tx.commit();
        }

        List<String> allKeys = Lists.newArrayList("hello", "number", "foo");
        List<Long> commitTimestamps = db.tx().getCommitTimestampsAfter(0L, 10);
        SerializationManager serman = db.getSerializationManager();
        for(Long commitTimestamp : commitTimestamps){
            ChronoDBTransaction tx = db.tx(commitTimestamp);
            for(String key : allKeys) {
                Object original = tx.get(key);
                byte[] binary = tx.getBinary(key);
                if(original == null){
                    assertNull(binary);
                }else{
                    assertEquals(original, serman.deserialize(binary));
                }
            }
        }
    }
}
