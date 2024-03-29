package org.chronos.chronodb.test.cases.engine.transaction;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.internal.api.TemporalKeyValueStore;
import org.chronos.chronodb.internal.impl.engines.base.AbstractTemporalKeyValueStore;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.utils.NamedPayload;
import org.chronos.chronodb.test.cases.util.model.payload.NamedPayloadNameIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class RollbackTest extends AllChronoDBBackendsTest {

    @Test
    public void rollbackOnPrimaryIndexWorks() {
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx = db.tx();
        tx.put("np1", NamedPayload.create1KB("np1"));
        tx.put("np2", NamedPayload.create1KB("np2"));
        tx.commit();
        long timeAfterFirstCommit = tx.getTimestamp();
        tx.put("np3", NamedPayload.create1KB("np3"));
        tx.commit();

        assertEquals(3, tx.keySet().size());

        // perform a rollback
        TemporalKeyValueStore tkvs = this.getMasterTkvs(db);
        AbstractTemporalKeyValueStore aTKVS = (AbstractTemporalKeyValueStore) tkvs;
        aTKVS.performRollbackToTimestamp(timeAfterFirstCommit,
            Collections.singleton(ChronoDBConstants.DEFAULT_KEYSPACE_NAME), true);

        // now, let's open a new transaction
        ChronoDBTransaction tx2 = db.tx();

        // assert that we have indeed performed a rollback
        assertEquals(timeAfterFirstCommit, tx2.getTimestamp());

        // assert that the rolled-back data is now gone
        assertNull(tx2.get("np3"));
    }

    @Test
    public void rollbackOverriddenEntryWorks() {
        ChronoDB db = this.getChronoDB();
        ChronoDBTransaction tx = db.tx();
        tx.put("np1", NamedPayload.create1KB("np1"));
        tx.put("np2", NamedPayload.create1KB("np2"));
        tx.commit();
        long timeAfterFirstCommit = tx.getTimestamp();
        tx.put("np2", NamedPayload.create1KB("newName"));
        tx.commit();

        assertEquals(2, tx.keySet().size());

        // perform a rollback
        TemporalKeyValueStore tkvs = this.getMasterTkvs(db);
        AbstractTemporalKeyValueStore aTKVS = (AbstractTemporalKeyValueStore) tkvs;
        aTKVS.performRollbackToTimestamp(timeAfterFirstCommit,
            Collections.singleton(ChronoDBConstants.DEFAULT_KEYSPACE_NAME), true);

        // now, let's open a new transaction
        ChronoDBTransaction tx2 = db.tx();

        // assert that we have indeed performed a rollback
        assertEquals(timeAfterFirstCommit, tx2.getTimestamp());

        // assert that the rolled-back data is now gone
        assertEquals("np2", ((NamedPayload) tx.get("np2")).getName());
    }

    @Test
    public void rollbackOnSecondaryIndexWorks() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("name").withIndexer(new NamedPayloadNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();
        ChronoDBTransaction tx = db.tx();
        tx.put("np1", NamedPayload.create1KB("np1"));
        tx.put("np2", NamedPayload.create1KB("np2"));
        tx.commit();
        long timeAfterFirstCommit = tx.getTimestamp();
        tx.put("np3", NamedPayload.create1KB("np3"));
        tx.commit();

        assertEquals(3, tx.keySet().size());

        // perform a rollback
        TemporalKeyValueStore tkvs = this.getMasterTkvs(db);
        AbstractTemporalKeyValueStore aTKVS = (AbstractTemporalKeyValueStore) tkvs;
        aTKVS.performRollbackToTimestamp(timeAfterFirstCommit,
            Collections.singleton(ChronoDBConstants.DEFAULT_KEYSPACE_NAME), true);

        // now, let's open a new transaction
        ChronoDBTransaction tx2 = db.tx();

        // assert that we have indeed performed a rollback
        assertEquals(timeAfterFirstCommit, tx2.getTimestamp());

        // assert that the data is gone from the secondary index
        assertEquals(2, tx2.find().inDefaultKeyspace().where("name").startsWith("np").count());
    }
}
