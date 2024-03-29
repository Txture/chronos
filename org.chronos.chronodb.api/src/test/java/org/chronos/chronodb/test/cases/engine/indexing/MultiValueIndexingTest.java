package org.chronos.chronodb.test.cases.engine.indexing;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.indexing.StringIndexer;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class MultiValueIndexingTest extends AllChronoDBBackendsTest {

    @Test
    public void multiValuedIndexRetrievalWorks() {
        ChronoDB db = this.getChronoDB();
        IndexManager indexManager = db.getIndexManager();
        db.getIndexManager().createIndex().withName("name").withIndexer(new TestBeanIndexer()).onMaster().acrossAllTimestamps().build();
        indexManager.reindexAll();

        ChronoDBTransaction tx = db.tx();
        tx.put("tb1", new TestBean("TB1", "First", "Second"));
        tx.put("tb2", new TestBean("TB2", "Second", "Third"));
        tx.put("tb3", new TestBean("TB3"));
        tx.put("tb4", new TestBean("TB4", "First"));
        tx.commit();

        Set<Object> values = tx.find().inDefaultKeyspace().where("name").isEqualTo("First").getValuesAsSet();
        // should contain TestBean 1 and TestBean 4
        assertEquals(2, values.size());
        Set<String> allNames = values.stream().map(tb -> ((TestBean) tb).getNames()).flatMap(Collection::stream).collect(Collectors.toSet());
        assertTrue(allNames.contains("TB1"));
        assertTrue(allNames.contains("TB4"));
        assertFalse(allNames.contains("TB2"));
        assertFalse(allNames.contains("TB3"));
    }

    @Test
    public void multiValuedIndexingTemporalAddValueWorks() {
        ChronoDB db = this.getChronoDB();
        IndexManager indexManager = db.getIndexManager();
        db.getIndexManager().createIndex().withName("name").withIndexer(new TestBeanIndexer()).onMaster().acrossAllTimestamps().build();
        indexManager.reindexAll();

        ChronoDBTransaction tx = db.tx();
        tx.put("tb1", new TestBean("TB1", "First", "Second"));
        tx.put("tb2", new TestBean("TB2", "Second", "Third"));
        tx.put("tb3", new TestBean("TB3"));
        tx.put("tb4", new TestBean("TB4", "First"));
        tx.commit();

        long afterCommit1 = tx.getTimestamp();

        // add an index value to TB1
        tx.put("tb1", new TestBean("TB1", "First", "Second", "Third"));
        tx.commit();

        // assert that:
        // - after the first commit, there is only one match for "Third"
        // - after the second commit, there are two matches for "Third"

        ChronoDBTransaction txAfter1 = db.tx(afterCommit1);
        Set<Object> values = txAfter1.find().inDefaultKeyspace().where("name").isEqualTo("Third").getValuesAsSet();
        assertEquals(1, values.size());

        ChronoDBTransaction txAfter2 = db.tx();
        values = txAfter2.find().inDefaultKeyspace().where("name").isEqualTo("Third").getValuesAsSet();
        assertEquals(2, values.size());
    }

    @Test
    public void multiValuedIndexingTemporalRemoveValueWorks() {
        ChronoDB db = this.getChronoDB();
        IndexManager indexManager = db.getIndexManager();
        db.getIndexManager().createIndex().withName("name").withIndexer(new TestBeanIndexer()).onMaster().acrossAllTimestamps().build();
        indexManager.reindexAll();

        ChronoDBTransaction tx = db.tx();
        tx.put("tb1", new TestBean("TB1", "First", "Second"));
        tx.put("tb2", new TestBean("TB2", "Second", "Third"));
        tx.put("tb3", new TestBean("TB3"));
        tx.put("tb4", new TestBean("TB4", "First"));
        tx.commit();

        long afterCommit1 = tx.getTimestamp();

        // remove an index value from TB1
        tx.put("tb1", new TestBean("TB1", "Second"));
        tx.commit();

        // assert that:
        // - after the first commit, there are two matches for "Second"
        // - after the second commit, there is only one match for "Second"

        ChronoDBTransaction txAfter1 = db.tx(afterCommit1);
        Set<Object> values = txAfter1.find().inDefaultKeyspace().where("name").isEqualTo("First").getValuesAsSet();
        assertEquals(2, values.size());

        ChronoDBTransaction txAfter2 = db.tx();
        values = txAfter2.find().inDefaultKeyspace().where("name").isEqualTo("First").getValuesAsSet();
        assertEquals(1, values.size());
    }

    public static class TestBean {

        private final Set<String> names = Sets.newHashSet();

        @SuppressWarnings("unused")
        public TestBean() {
            // default constructor for serialization
        }

        public TestBean(final String... names) {
            Collections.addAll(this.names, names);
        }

        public Set<String> getNames() {
            return Collections.unmodifiableSet(this.names);
        }

    }

    public static class TestBeanIndexer implements StringIndexer {

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
            return object instanceof TestBean;
        }

        @Override
        public Set<String> getIndexValues(final Object object) {
            TestBean testBean = (TestBean) object;
            return testBean.getNames();
        }

    }

}
