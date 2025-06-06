package org.chronos.chronograph.test.cases.transaction;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.exceptions.ChronoDBCommitException;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.builder.query.CP;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class IncrementalGraphCommitTest extends AllChronoGraphBackendsTest {

    @Test
    public void canCommitDirectlyAfterIncrementalCommit() {
        ChronoGraph g = this.getGraph();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            g.tx().commitIncremental();

            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            g.tx().commitIncremental();
            g.tx().commit();
        } finally {
            g.tx().rollback();
        }
        // assert that the data was written correctly
        assertEquals(6, Iterators.size(g.vertices()));
    }

    @Test
    public void canCommitIncrementally() {
        ChronoGraph g = this.getGraph();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            g.tx().commitIncremental();

            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            g.tx().commitIncremental();

            g.addVertex("name", "seven");
            g.addVertex("name", "eight");
            g.addVertex("name", "nine");
            g.tx().commit();
        } finally {
            g.tx().rollback();
        }
        assertEquals(9, Iterators.size(g.vertices()));
    }

    @Test
    public void incrementalCommitTransactionCanReadItsOwnModifications() {
        ChronoGraph g = this.getGraph();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            g.tx().commitIncremental();
            assertEquals(3, Iterators.size(g.vertices()));
            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            g.tx().commitIncremental();
            assertEquals(6, Iterators.size(g.vertices()));
            g.addVertex("name", "seven");
            g.addVertex("name", "eight");
            g.addVertex("name", "nine");
            g.tx().commit();
        } finally {
            g.tx().rollback();
        }
        // assert that the data was written correctly
        assertEquals(9, Iterators.size(g.vertices()));
    }

    @Test
    public void incrementalCommitTransactionCanReadItsOwnModificationsInIndexer() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            g.tx().commitIncremental();
            assertEquals(3, Iterators.size(g.vertices()));
            assertEquals(1, g.traversal().V().has("name", "one").toSet().size());
            assertEquals(2, g.traversal().V().has("name", CP.contains("e")).toSet().size());

            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            g.tx().commitIncremental();
            assertEquals(6, Iterators.size(g.vertices()));
            assertEquals(1, g.traversal().V().has("name", "one").toSet().size());
            assertEquals(3, g.traversal().V().has("name", CP.contains("e")).toSet().size());

            g.addVertex("name", "seven");
            g.addVertex("name", "eight");
            g.addVertex("name", "nine");
            g.tx().commit();
        } finally {
            g.tx().rollback();
        }
        // assert that the data was written correctly
        assertEquals(9, Iterators.size(g.vertices()));
        assertEquals(1, g.traversal().V().has("name", "one").toSet().size());
        assertEquals(6, g.traversal().V().has("name", CP.contains("e")).toSet().size());
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void incrementalCommitTransactionCanReadItsOwnModificationsInIndexerWithQueryCaching() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            g.tx().commitIncremental();
            assertEquals(3, Iterators.size(g.vertices()));
            assertEquals(1, g.traversal().V().has("name", "one").toSet().size());
            assertEquals(2, g.traversal().V().has("name", CP.contains("e")).toSet().size());

            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            g.tx().commitIncremental();
            assertEquals(6, Iterators.size(g.vertices()));
            assertEquals(1, g.traversal().V().has("name", "one").toSet().size());
            assertEquals(3, g.traversal().V().has("name", CP.contains("e")).toSet().size());

            g.addVertex("name", "seven");
            g.addVertex("name", "eight");
            g.addVertex("name", "nine");
            g.tx().commit();
        } finally {
            g.tx().rollback();
        }
        // assert that the data was written correctly
        assertEquals(9, Iterators.size(g.vertices()));
        assertEquals(1, g.traversal().V().has("name", "one").toSet().size());
        assertEquals(6, g.traversal().V().has("name", CP.contains("e")).toSet().size());
    }

    @Test
    public void cannotCommitOnOtherTransactionWhileIncrementalCommitProcessIsRunning() {
        ChronoGraph g = this.getGraph();
        ChronoGraph tx = g.tx().createThreadedTx();
        try {
            tx.addVertex("name", "one");
            tx.addVertex("name", "two");
            tx.addVertex("name", "three");
            tx.tx().commitIncremental();
            assertEquals(3, Iterators.size(tx.vertices()));
            tx.addVertex("name", "four");
            tx.addVertex("name", "five");
            tx.addVertex("name", "six");
            tx.tx().commitIncremental();

            // simulate a second transaction
            ChronoGraph tx2 = g.tx().createThreadedTx();
            tx2.addVertex("name", "thirteen");
            try {
                tx2.tx().commit();
                fail("Managed to commit on other transaction while incremental commit is active!");
            } catch (ChronoDBCommitException expected) {
                // pass
            }

            // continue with the incremental commit

            assertEquals(6, Iterators.size(tx.vertices()));
            tx.addVertex("name", "seven");
            tx.addVertex("name", "eight");
            tx.addVertex("name", "nine");
            tx.tx().commit();
        } finally {
            tx.tx().rollback();
        }
        // assert that the data was written correctly
        assertEquals(9, Iterators.size(g.vertices()));
    }

    @Test
    public void incrementalCommitsAppearAsSingleCommitInHistory() {
        ChronoGraph g = this.getGraph();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            Vertex alpha = g.addVertex("name", "alpha", "value", 100);
            g.tx().commitIncremental();

            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            // update alpha
            alpha.property("value", 200);
            g.tx().commitIncremental();

            g.addVertex("name", "seven");
            g.addVertex("name", "eight");
            g.addVertex("name", "nine");
            // update alpha
            alpha.property("value", 300);
            g.tx().commit();
        } finally {
            g.tx().rollback();
        }
        // assert that the data was written correctly
        assertEquals(10, Iterators.size(g.vertices()));
        // find the alpha vertex
        Vertex alpha = g.traversal().V().has("name", "alpha").toSet().iterator().next();
        // assert that alpha was written only once in the versioning history
        Iterator<Long> historyOfAlpha = g.getVertexHistory(alpha);
        assertEquals(1, Iterators.size(historyOfAlpha));
        // assert that all keys have the same timestamp
        Set<Long> timestamps = Sets.newHashSet();
        g.vertices().forEachRemaining(vertex -> {
            Iterator<Long> history = g.getVertexHistory(vertex);
            timestamps.add(Iterators.getOnlyElement(history));
        });
        assertEquals(1, timestamps.size());
    }

    @Test
    public void rollbackDuringIncrementalCommitWorks() {
        ChronoGraph g = this.getGraph();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            g.tx().commitIncremental();

            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            g.tx().commitIncremental();
            // simulate user error
            throw new RuntimeException("User error");
        } catch (RuntimeException expected) {
        } finally {
            g.tx().rollback();
        }
        // assert that the data was rolled back correctly
        assertEquals(0, Iterators.size(g.vertices()));
    }

    @Test
    public void canCommitRegularlyAfterCompletedIncrementalCommitProcess() {
        ChronoGraph g = this.getGraph();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            g.tx().commitIncremental();

            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            g.tx().commitIncremental();

            g.addVertex("name", "seven");
            g.addVertex("name", "eight");
            g.addVertex("name", "nine");
            g.tx().commit();
        } finally {
            g.tx().rollback();
        }
        assertEquals(9, Iterators.size(g.vertices()));

        // can commit on a different transaction
        ChronoGraph tx2 = g.tx().createThreadedTx();
        tx2.addVertex("name", "fourtytwo");
        tx2.tx().commit();

        // can commit on the same transaction
        g.addVertex("name", "fourtyseven");
        g.tx().commit();

        assertEquals(11, Iterators.size(g.vertices()));
    }

    @Test
    public void canCommitRegularlyAfterCanceledIncrementalCommitProcess() {
        ChronoGraph g = this.getGraph();
        try {
            g.addVertex("name", "one");
            g.addVertex("name", "two");
            g.addVertex("name", "three");
            g.tx().commitIncremental();

            g.addVertex("name", "four");
            g.addVertex("name", "five");
            g.addVertex("name", "six");
            g.tx().commitIncremental();

            g.addVertex("name", "seven");
            g.addVertex("name", "eight");
            g.addVertex("name", "nine");
        } finally {
            g.tx().rollback();
        }
        assertEquals(0, Iterators.size(g.vertices()));

        // can commit on a different transaction
        ChronoGraph tx2 = g.tx().createThreadedTx();
        tx2.addVertex("name", "fourtytwo");
        tx2.tx().commit();

        // can commit on the same transaction
        g.addVertex("name", "fourtyseven");
        g.tx().commit();

        assertEquals(2, Iterators.size(g.vertices()));
    }

    @Test
    @SuppressWarnings("unused")
    public void secondaryIndexingIsCorrectDuringIncrementalCommit() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("firstName").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("lastName").acrossAllTimestamps().build();
        g.tx().commit();
        try {
            // add three persons
            Vertex p1 = g.addVertex(T.id, "p1", "firstName", "John", "lastName", "Doe");
            Vertex p2 = g.addVertex(T.id, "p2", "firstName", "John", "lastName", "Smith");
            Vertex p3 = g.addVertex(T.id, "p3", "firstName", "Jane", "lastName", "Doe");

            // perform the incremental commit
            g.tx().commitIncremental();

            // make sure that we can find them
            assertEquals(2, (long)g.traversal().V().has("firstName", CP.eqIgnoreCase("john")).count().next());
            assertEquals(2, (long)g.traversal().V().has("lastName", CP.eqIgnoreCase("doe")).count().next());

            // change Jane's and John's first names
            p3.property("firstName", "Jayne");
            p1.property("firstName", "Jack");

            // perform the incremental commit
            g.tx().commitIncremental();

            // make sure that we can't find John any longer
            assertEquals(1, (long)g.traversal().V().has("firstName", CP.eqIgnoreCase("john")).count().next());
            assertEquals(2, (long)g.traversal().V().has("lastName", CP.eqIgnoreCase("doe")).count().next());

            // change Jack's first name (yet again)
            p1.property("firstName", "Joe");

            // perform the incremental commit
            g.tx().commitIncremental();

            // make sure that we can't find Jack Doe any longer
            assertEquals(0, (long)g.traversal().V().has("firstName", CP.eqIgnoreCase("jack")).count().next());
            // john smith should still be there
            assertEquals(1, (long)g.traversal().V().has("firstName", CP.eqIgnoreCase("john")).count().next());
            // we still have jayne doe and joe doe
            assertEquals(2, (long)g.traversal().V().has("lastName", CP.eqIgnoreCase("doe")).count().next());
            // jayne should be in the first name index as well
            assertEquals(1, (long)g.traversal().V().has("firstName", CP.eqIgnoreCase("jayne")).count().next());

            // delete Joe
            p1.remove();

            // make sure that joe's gone
            assertEquals(0, (long)g.traversal().V().has("firstName", "Joe").count().next());

            // do the full commit
            g.tx().commit();
        } finally {
            g.tx().rollback();
        }

        // in the end, there should be john smith and jayne doe
        assertEquals(1, (long)g.traversal().V().has("firstName", CP.eqIgnoreCase("john")).count().next());
        assertEquals(1, (long)g.traversal().V().has("lastName", CP.eqIgnoreCase("doe")).count().next());
    }

    @Test
    public void getAndExistsWorkProperlyAfterDeletionDuringIncrementalCommit() {
        ChronoGraph g = this.getGraph();
        { // add some base data
            g.tx().open();
            g.addVertex(T.id, "a", "name", "Hello");
            g.addVertex(T.id, "b", "name", "World");
            g.tx().commit();
        }
        { // do the incremental commit process
            g.tx().open();
            assertTrue(Iterators.getOnlyElement(g.vertices("a"), null) != null);
            assertTrue(Iterators.getOnlyElement(g.vertices("b"), null) != null);
            assertEquals("Hello", Iterators.getOnlyElement(g.vertices("a")).value("name"));
            assertEquals("World", Iterators.getOnlyElement(g.vertices("b")).value("name"));

            g.addVertex(T.id, "c", "name", "Foo");
            g.addVertex(T.id, "d", "name", "Bar");

            g.tx().commitIncremental();

            assertTrue(Iterators.getOnlyElement(g.vertices("a"), null) != null);
            assertTrue(Iterators.getOnlyElement(g.vertices("b"), null) != null);
            assertTrue(Iterators.getOnlyElement(g.vertices("c"), null) != null);
            assertTrue(Iterators.getOnlyElement(g.vertices("d"), null) != null);

            Iterators.getOnlyElement(g.vertices("a"), null).remove();

            g.tx().commitIncremental();

            assertNull(Iterators.getOnlyElement(g.vertices("a"), null));

            g.tx().commit();
        }

        g.tx().open();
        assertNull(Iterators.getOnlyElement(g.vertices("a"), null));
    }

    @Test
    public void canRecreateVertexDuringIncrementalCommitProcess() {
        ChronoGraph g = this.getGraph();
        { // add some base data
            g.tx().open();
            g.addVertex(T.id, "a", "name", "Hello");
            g.addVertex(T.id, "b", "name", "World");
            g.tx().commit();
        }
        { // do the incremental commit process
            g.tx().open();
            assertTrue(Iterators.getOnlyElement(g.vertices("a"), null) != null);
            assertTrue(Iterators.getOnlyElement(g.vertices("b"), null) != null);
            assertEquals("Hello", Iterators.getOnlyElement(g.vertices("a")).value("name"));
            assertEquals("World", Iterators.getOnlyElement(g.vertices("b")).value("name"));

            g.addVertex(T.id, "c", "name", "Foo");
            g.addVertex(T.id, "d", "name", "Bar");

            g.tx().commitIncremental();

            assertTrue(Iterators.getOnlyElement(g.vertices("a"), null) != null);
            assertTrue(Iterators.getOnlyElement(g.vertices("b"), null) != null);
            assertTrue(Iterators.getOnlyElement(g.vertices("c"), null) != null);
            assertTrue(Iterators.getOnlyElement(g.vertices("d"), null) != null);

            Iterators.getOnlyElement(g.vertices("a"), null).remove();

            g.tx().commitIncremental();

            assertNull(Iterators.getOnlyElement(g.vertices("a"), null));

            // recreate a
            g.addVertex(T.id, "a", "name", "BornAgain");

            g.tx().commitIncremental();
            assertTrue(Iterators.getOnlyElement(g.vertices("a"), null) != null);
            assertEquals("BornAgain", Iterators.getOnlyElement(g.vertices("a")).value("name"));

            g.tx().commit();
        }
        g.tx().open();
        assertTrue(Iterators.getOnlyElement(g.vertices("a"), null) != null);
        assertEquals("BornAgain", Iterators.getOnlyElement(g.vertices("a")).value("name"));
    }

}
