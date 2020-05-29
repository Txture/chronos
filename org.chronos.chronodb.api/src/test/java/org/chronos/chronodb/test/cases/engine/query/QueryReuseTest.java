package org.chronos.chronodb.test.cases.engine.query;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.api.key.QualifiedKey;
import org.chronos.chronodb.internal.api.query.ChronoDBQuery;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class QueryReuseTest extends AllChronoDBBackendsTest {

    @Test
    public void queryReuseWorks() {
        ChronoDB db = this.getChronoDB();
        StringIndexer integerIndexer = new StringIndexer() {

            @Override
            public int hashCode() {
                return super.hashCode();
            }

            @Override
            public boolean equals(final Object obj) {
                return super.equals(obj);
            }

            @Override
            public boolean canIndex(final Object object) {
                return object instanceof Integer;
            }

            @Override
            public Set<String> getIndexValues(final Object object) {
                return Collections.singleton(((Integer) object).toString());
            }

        };
        db.getIndexManager().addIndexer("integer", integerIndexer);
        ChronoDBTransaction tx = db.tx();
        tx.put("First", 123);
        tx.put("Second", 456);
        tx.put("Third", 123);
        tx.commit();
        long writeTimestamp = tx.getTimestamp();

        this.sleep(5);

        tx.put("First", 789);
        tx.commit();

        // buildLRU the query
        ChronoDBQuery query = tx.find().inDefaultKeyspace().where("integer").isEqualTo("123").toQuery();

        // check that before the second commit, we get 2 results by running the query
        {
            ChronoDBTransaction txBefore = db.tx(writeTimestamp);
            Iterator<QualifiedKey> keys = txBefore.find(query).getKeys();
            Set<String> keySet = Sets.newHashSet(keys).stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertTrue(keySet.contains("First"));
            assertTrue(keySet.contains("Third"));
            assertEquals(2, keySet.size());
        }

        // check that after the second commit, we get 1 result by running the query
        {
            ChronoDBTransaction txBefore = db.tx();
            Iterator<QualifiedKey> keys = txBefore.find(query).getKeys();
            Set<String> keySet = Sets.newHashSet(keys).stream().map(qKey -> qKey.getKey()).collect(Collectors.toSet());
            assertFalse(keySet.contains("First"));
            assertTrue(keySet.contains("Third"));
            assertEquals(1, keySet.size());
        }
    }

}
