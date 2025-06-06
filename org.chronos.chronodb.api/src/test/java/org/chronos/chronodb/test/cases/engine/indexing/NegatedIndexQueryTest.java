package org.chronos.chronodb.test.cases.engine.indexing;

import org.chronos.chronodb.api.ChronoDB;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.ChronoDBTransaction;
import org.chronos.chronodb.test.base.AllChronoDBBackendsTest;
import org.chronos.chronodb.test.cases.util.ReflectiveStringIndexer;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.google.common.base.Preconditions.*;

@Category(IntegrationTest.class)
public class NegatedIndexQueryTest extends AllChronoDBBackendsTest {

    @Test
    public void canEvaluateSimpleNegatedQuery() {
        ChronoDB db = this.getChronoDB();
        db.getIndexManager().createIndex().withName("name").withIndexer(new ReflectiveStringIndexer(TestBean.class, "name")).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("x").withIndexer(new ReflectiveStringIndexer(TestBeanA.class, "x")).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().createIndex().withName("y").withIndexer(new ReflectiveStringIndexer(TestBeanB.class, "y")).onMaster().acrossAllTimestamps().build();
        db.getIndexManager().reindexAll();

        { // add some data
            ChronoDBTransaction tx = db.tx();
            tx.put("a1", new TestBeanA("a", "x1"));
            tx.put("a2", new TestBeanA("b", "x2"));
            tx.put("a3", new TestBeanA("c", "x3"));
            tx.put("a4", new TestBeanA("d", "x4"));

            tx.put("b1", new TestBeanB("a", "y1"));
            tx.put("b2", new TestBeanB("b", "y2"));
            tx.put("b3", new TestBeanB("c", "y3"));
            tx.put("b4", new TestBeanB("d", "y4"));

            tx.commit();
        }

        assertKeysEqual("b1", "b2", "b3", "b4", db.tx().find().inDefaultKeyspace().where("x").notStartsWith("x"));
        assertKeysEqual("a1", "a2", "a3", "a4", "b1", "b2", "b3", "b4", db.tx().find().inDefaultKeyspace().where("x").isNotEqualTo("test"));
        assertKeysEqual("a2", "a3", "a4", "b1", "b2", "b3", "b4", db.tx().find().inDefaultKeyspace().where("x").notMatchesRegex("x1"));
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    @SuppressWarnings("unused")
    private static abstract class TestBean {

        private String name;

        protected TestBean() {
        }

        protected TestBean(final String name) {
            checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    @SuppressWarnings("unused")
    private static class TestBeanA extends TestBean {

        private String x;

        protected TestBeanA() {
        }

        protected TestBeanA(final String name, final String x) {
            super(name);
            this.x = x;
        }

        public String getX() {
            return this.x;
        }
    }

    @SuppressWarnings("unused")
    private static class TestBeanB extends TestBean {

        private String y;

        protected TestBeanB() {
        }

        public TestBeanB(final String name, final String y) {
            super(name);
            this.y = y;
        }

        public String getY() {
            return this.y;
        }
    }

}
