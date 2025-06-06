package org.chronos.chronodb.test.cases.engine.transaction;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.PutOption;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.utils.NamedPayload;
import org.chronos.chronodb.test.cases.util.model.payload.NamedPayloadNameIndexer;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PutOptionsTest extends AllChronoDBBackendsTest {

    @Test
    public void noIndexOptionWorks() {
        ChronoDB db = this.getChronoDB();
        // create an indexer
        db.getIndexManager().createIndex().withName("name").withIndexer(new NamedPayloadNameIndexer()).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        // assert that the index is empty
        // NOTE: This will also force lazy-initializing indexing backends to
        // produce an index.
        Set<QualifiedKey> keysAsSet = db.tx().find().inDefaultKeyspace().where("name").matchesRegex(".*").getKeysAsSet();
        assertTrue(keysAsSet.isEmpty());

        // add some data, with and without NO_INDEX option
        ChronoDBTransaction tx = db.tx();
        tx.put("one", NamedPayload.create1KB("one"));
        tx.put("two", NamedPayload.create1KB("two"));
        tx.put("three", NamedPayload.create1KB("three"), PutOption.NO_INDEX);
        tx.put("four", NamedPayload.create1KB("four"), PutOption.NO_INDEX);
        // commit the changes
        tx.commit();

        // a query on our secondary indexer should only reveal "one" and "two", but not "three" and "four"
        Set<QualifiedKey> keys = tx.find().inDefaultKeyspace().where("name").matchesRegex(".*").getKeysAsSet();
        Set<String> keySet = keys.stream().map(QualifiedKey::getKey).collect(Collectors.toSet());
        assertEquals(2, keySet.size());
        assertTrue(keySet.contains("one"));
        assertTrue(keySet.contains("two"));

        // make sure that our objects still exist outside the secondary indexer
        assertTrue(tx.exists("three"));
        assertTrue(tx.exists("four"));
        assertEquals("three", ((NamedPayload) tx.get("three")).getName());
        assertEquals("four", ((NamedPayload) tx.get("four")).getName());
    }

}
