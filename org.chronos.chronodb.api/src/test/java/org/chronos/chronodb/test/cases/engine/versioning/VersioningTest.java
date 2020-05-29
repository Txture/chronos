package org.chronos.chronodb.test.cases.engine.versioning;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class VersioningTest extends AllChronoDBBackendsTest {

    @Test
    public void testSimpleRW() {
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx = db.tx();
        tx.put("First", 123);
        tx.put("Second", 456);
        tx.commit();
        long writeTimestamp = tx.getTimestamp();
        this.sleep(5);
        tx.put("Third", 789);
        tx.commit();

        ChronoDBTransaction txBefore = db.tx(0L);
        assertEquals(false, txBefore.exists("First"));
        assertEquals(false, txBefore.exists("Second"));
        assertNull(txBefore.get("First"));
        assertNull(txBefore.get("Second"));
        assertEquals(false, txBefore.history("First").hasNext());
        assertEquals(false, txBefore.history("Second").hasNext());
        assertEquals(true, txBefore.keySet().isEmpty());

        ChronoDBTransaction txExact = db.tx(writeTimestamp);
        assertTrue(txExact.exists("First"));
        assertTrue(txExact.exists("Second"));
        assertEquals(123, (int) txExact.get("First"));
        assertEquals(456, (int) txExact.get("Second"));
        assertEquals(1, Iterators.size(txExact.history("First")));
        assertEquals(1, Iterators.size(txExact.history("Second")));
        assertEquals(writeTimestamp, (long) txExact.history("First").next());
        assertEquals(writeTimestamp, (long) txExact.history("Second").next());
        assertEquals(2, txExact.keySet().size());

        ChronoDBTransaction txAfter = db.tx(writeTimestamp + 1);
        assertTrue(txAfter.exists("First"));
        assertTrue(txAfter.exists("Second"));
        assertEquals(123, (int) txAfter.get("First"));
        assertEquals(456, (int) txAfter.get("Second"));
        assertEquals(1, Iterators.size(txAfter.history("First")));
        assertEquals(1, Iterators.size(txAfter.history("Second")));
        assertEquals(writeTimestamp, (long) txAfter.history("First").next());
        assertEquals(writeTimestamp, (long) txAfter.history("Second").next());
        assertEquals(2, txAfter.keySet().size());
    }

    @Test
    public void testOverrideOnce() {
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx1 = db.tx();
        tx1.put("First", 123);
        tx1.put("Second", 456);
        tx1.commit();
        long writeTimestamp1 = tx1.getTimestamp();

        this.sleep(5);

        ChronoDBTransaction tx2 = db.tx();
        tx2.put("First", 47);
        tx2.commit();
        long writeTimestamp2 = tx2.getTimestamp();
        this.sleep(5);
        tx2.put("Third", 789);
        tx2.commit();

        { // BEFORE
            ChronoDBTransaction tx = db.tx(0L);
            assertEquals(false, tx.exists("First"));
            assertEquals(false, tx.exists("Second"));
            assertNull(tx.get("First"));
            assertNull(tx.get("Second"));
            assertEquals(false, tx.history("First").hasNext());
            assertEquals(false, tx.history("Second").hasNext());
            assertEquals(true, tx.keySet().isEmpty());
        }
        { // EXACTLY AT WRITE 1
            ChronoDBTransaction tx = db.tx(writeTimestamp1);
            assertTrue(tx.exists("First"));
            assertTrue(tx.exists("Second"));
            assertEquals(123, (int) tx.get("First"));
            assertEquals(456, (int) tx.get("Second"));
            assertEquals(1, Iterators.size(tx.history("First")));
            assertEquals(1, Iterators.size(tx.history("Second")));
            assertEquals(writeTimestamp1, (long) tx.history("First").next());
            assertEquals(writeTimestamp1, (long) tx.history("Second").next());
            assertEquals(2, tx.keySet().size());
        }
        { // BETWEEN WRITES
            ChronoDBTransaction tx = db.tx(writeTimestamp1 + 1);
            assertTrue(tx.exists("First"));
            assertTrue(tx.exists("Second"));
            assertEquals(123, (int) tx.get("First"));
            assertEquals(456, (int) tx.get("Second"));
            assertEquals(1, Iterators.size(tx.history("First")));
            assertEquals(1, Iterators.size(tx.history("Second")));
            assertEquals(writeTimestamp1, (long) tx.history("First").next());
            assertEquals(writeTimestamp1, (long) tx.history("Second").next());
            assertEquals(2, tx.keySet().size());
        }
        { // EXACTLY AT WRITE 2
            ChronoDBTransaction tx = db.tx(writeTimestamp2);
            assertTrue(tx.exists("First"));
            assertTrue(tx.exists("Second"));
            assertEquals(47, (int) tx.get("First"));
            assertEquals(456, (int) tx.get("Second"));
            assertEquals(2, Iterators.size(tx.history("First")));
            assertEquals(1, Iterators.size(tx.history("Second")));
            Iterator<Long> iterator = tx.history("First");
            List<Long> timestamps = Lists.newArrayList(iterator);
            assertEquals(2, timestamps.size());
            assertTrue(timestamps.contains(writeTimestamp1));
            assertTrue(timestamps.contains(writeTimestamp2));
            assertEquals(writeTimestamp1, (long) tx.history("Second").next());
            assertEquals(2, tx.keySet().size());
        }
        { // AFTER WRITE 2
            ChronoDBTransaction tx = db.tx(writeTimestamp2 + 1);
            assertTrue(tx.exists("First"));
            assertTrue(tx.exists("Second"));
            assertEquals(47, (int) tx.get("First"));
            assertEquals(456, (int) tx.get("Second"));
            Iterator<Long> iterator = tx.history("First");
            List<Long> timestamps = Lists.newArrayList(iterator);
            assertEquals(2, timestamps.size());
            assertTrue(timestamps.contains(writeTimestamp1));
            assertTrue(timestamps.contains(writeTimestamp2));
            assertEquals(writeTimestamp1, (long) tx.history("Second").next());
            assertEquals(2, tx.keySet().size());
        }
        // wait for a bit to ensure that the operation is complete
        this.sleep(5);
        { // NOW
            ChronoDBTransaction tx = db.tx();
            assertTrue(tx.exists("First"));
            assertTrue(tx.exists("Second"));
            assertEquals(47, (int) tx.get("First"));
            assertEquals(456, (int) tx.get("Second"));
            Iterator<Long> iterator = tx.history("First");
            List<Long> timestamps = Lists.newArrayList(iterator);
            assertEquals(2, timestamps.size());
            assertTrue(timestamps.contains(writeTimestamp1));
            assertTrue(timestamps.contains(writeTimestamp2));
            assertEquals(writeTimestamp1, (long) tx.history("Second").next());
            // this includes the "third" key
            assertEquals(3, tx.keySet().size());
        }
    }

    @Test
    public void testOverrideMany() {
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx1 = db.tx();
        tx1.put("First", 123);
        tx1.put("Second", 456);
        tx1.commit();

        sleep(5);

        // overwrite "First" value 10 times
        List<Long> writeTimestamps = Lists.newArrayList();
        int value = 123;
        for (int i = 0; i < 11; i++) {
            ChronoDBTransaction tx = db.tx();
            value++;
            tx.put("First", value);
            tx.commit();
            writeTimestamps.add(tx.getTimestamp());
            sleep(5);
        }

        {// BEFORE
            ChronoDBTransaction tx = db.tx(0L);
            assertEquals(false, tx.exists("First"));
            assertEquals(false, tx.exists("Second"));
            assertNull(tx.get("First"));
            assertNull(tx.get("Second"));
            assertEquals(false, tx.history("First").hasNext());
            assertEquals(false, tx.history("Second").hasNext());
            assertEquals(true, tx.keySet().isEmpty());
        }

        // check the remaining timestamps
        for (int i = 0; i < 10; i++) {
            long timestamp = writeTimestamps.get(i);
            System.out.println("Investigating timestamp #" + i + " (" + timestamp + ")...");
            {// BEFORE
                ChronoDBTransaction tx = db.tx(timestamp - 1);
                assertTrue(tx.exists("First"));
                assertTrue(tx.exists("Second"));
                assertEquals(123 + i, (int) tx.get("First"));
                assertEquals(456, (int) tx.get("Second"));
                assertEquals(i + 1, Iterators.size(tx.history("First")));
                assertEquals(1, Iterators.size(tx.history("Second")));
                assertEquals(2, tx.keySet().size());
            }
            {// EXACT
                ChronoDBTransaction tx = db.tx(timestamp);
                assertTrue(tx.exists("First"));
                assertTrue(tx.exists("Second"));
                assertEquals(123 + i + 1, (int) tx.get("First"));
                assertEquals(456, (int) tx.get("Second"));
                assertEquals(i + 2, Iterators.size(tx.history("First")));
                assertEquals(1, Iterators.size(tx.history("Second")));
                assertEquals(2, tx.keySet().size());
            }
            {// AFTER
                ChronoDBTransaction tx = db.tx(timestamp + 1);
                assertTrue(tx.exists("First"));
                assertTrue(tx.exists("Second"));
                assertEquals(123 + i + 1, (int) tx.get("First"));
                assertEquals(456, (int) tx.get("Second"));
                assertEquals(i + 2, Iterators.size(tx.history("First")));
                assertEquals(1, Iterators.size(tx.history("Second")));
                assertEquals(2, tx.keySet().size());
            }
        }
    }


    @Test
    public void canLimitHistory(){
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx1 = db.tx();
        tx1.put("First", 123);
        tx1.put("Second", 456);
        tx1.commit();

        sleep(5);

        // overwrite "First" value 10 times
        List<Long> writeTimestamps = Lists.newArrayList();
        int value = 123;
        for (int i = 0; i < 11; i++) {
            ChronoDBTransaction tx = db.tx();
            value++;
            tx.put("First", value);
            tx.commit();
            writeTimestamps.add(tx.getTimestamp());
            sleep(5);
        }

        assertEquals(9, Iterators.size(db.tx().history("First", 0, writeTimestamps.get(7))));
        assertEquals(5, Iterators.size(db.tx().history("First", writeTimestamps.get(3), writeTimestamps.get(7))));
        assertEquals(8, Iterators.size(db.tx().history("First", writeTimestamps.get(3), db.tx().getTimestamp())));
        assertEquals(10, Iterators.size(db.tx().history("First", writeTimestamps.get(0), writeTimestamps.get(9))));
    }

    @Test
    public void canChangeHistoryOrdering(){
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx1 = db.tx();
        tx1.put("First", 123);
        tx1.put("Second", 456);
        tx1.commit();

        sleep(5);

        // overwrite "First" value 10 times
        List<Long> writeTimestamps = Lists.newArrayList();
        int value = 123;
        for (int i = 0; i < 11; i++) {
            ChronoDBTransaction tx = db.tx();
            value++;
            tx.put("First", value);
            tx.commit();
            writeTimestamps.add(tx.getTimestamp());
            sleep(5);
        }

        assertThat(
            Lists.newArrayList(db.tx().history("First", writeTimestamps.get(0), writeTimestamps.get(2), Order.ASCENDING)),
            contains(
                writeTimestamps.get(0),
                writeTimestamps.get(1),
                writeTimestamps.get(2)
            )
        );

        assertThat(
            Lists.newArrayList(db.tx().history("First", writeTimestamps.get(0), writeTimestamps.get(2), Order.DESCENDING)),
            contains(
                writeTimestamps.get(2),
                writeTimestamps.get(1),
                writeTimestamps.get(0)
            )
        );
    }

    @Test
    public void canQueryHistoryAcrossBranches(){
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx1 = db.tx();
        tx1.put("a", 1);
        tx1.put("b", 1);
        long commit1 = tx1.commit();

        db.getBranchManager().createBranch("b1");

        ChronoDBTransaction tx2 = db.tx("b1");
        tx2.put("a", 2);
        tx2.put("c", 1);
        long commit2 = tx2.commit();

        // do another transaction on master after branching away
        ChronoDBTransaction tx3 = db.tx();
        tx3.put("z", 1);
        tx3.remove("a");
        tx3.commit();

        db.getBranchManager().createBranch("b1", "b2");

        ChronoDBTransaction tx4 = db.tx("b2");
        tx4.remove("a");
        tx4.put("b", 2);
        tx4.put("d", 1);
        long commit4 = tx4.commit();

        // do another transaction on b1
        ChronoDBTransaction tx5 = db.tx("b1");
        tx5.put("y", 1);
        tx5.remove("b");
        tx5.commit();

        // create a new branch that contains nothing
        db.getBranchManager().createBranch("b2", "b3");

        // query the history of "a" on b2
        assertThat(Lists.newArrayList(db.tx("b3").history("a")), contains(commit4, commit2, commit1));

        // query in ascending order
        assertThat(Lists.newArrayList(db.tx("b3").history("a", Order.ASCENDING)), contains(commit1, commit2, commit4));

        // query history with bounds
        assertThat(Lists.newArrayList(db.tx("b3").history("a", commit1, commit2, Order.DESCENDING)), contains(commit2, commit1));
        assertThat(Lists.newArrayList(db.tx("b3").history("a", commit1, commit2, Order.ASCENDING)), contains(commit1, commit2));
    }

}
