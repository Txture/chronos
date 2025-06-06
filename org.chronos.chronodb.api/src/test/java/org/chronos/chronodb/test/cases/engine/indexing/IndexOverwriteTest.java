package org.chronos.chronodb.test.cases.engine.indexing;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.cases.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.test.utils.NamedPayload;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class IndexOverwriteTest extends AllChronoDBBackendsTest {

    @Test
    public void overwrittenIndexEntryIsNoLongerPresent() {
        ChronoDB db = this.getChronoDB();
        StringIndexer nameIndexer = new NamedPayloadNameIndexer();
        db.getIndexManager().createIndex().withName("name").withIndexer(nameIndexer).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        // generate and insert test data
        NamedPayload np1 = NamedPayload.create1KB("Hello World");
        NamedPayload np2 = NamedPayload.create1KB("Foo Bar");
        NamedPayload np3 = NamedPayload.create1KB("Foo Baz");
        ChronoDBTransaction tx = db.tx();
        tx.put("np1", np1);
        tx.put("np2", np2);
        tx.put("np3", np3);
        tx.commit();
        long afterFirstWrite = tx.getTimestamp();

        // then, rename np1 and commit it (same key -> second version)
        np1 = NamedPayload.create1KB("Overwritten");
        tx.put("np1", np1);
        tx.commit();

        // open a read transaction after the first write
        ChronoDBTransaction tx2 = db.tx(afterFirstWrite);
        // assert that the "hello world" element is there
        long count = tx2.find().inDefaultKeyspace().where("name").isEqualTo("Hello World").count();
        assertEquals(1, count);

        // open a read transaction on "now" and assert that it is gone
        ChronoDBTransaction tx3 = db.tx();
        long count2 = tx3.find().inDefaultKeyspace().where("name").isEqualTo("Hello World").count();
        assertEquals(0, count2);
        // however, the "overwritten" name should appear
        long count3 = tx3.find().inDefaultKeyspace().where("name").isEqualTo("Overwritten").count();
        assertEquals(1, count3);
    }

}
