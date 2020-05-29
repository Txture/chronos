package org.chronos.chronograph.test.cases.configuration;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.chronos.chronodb.inmemory.InMemoryChronoDB;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.google.common.base.Preconditions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ChronoGraphConfigurationTest extends AllChronoGraphBackendsTest {

    @Test
    public void chronoGraphConfigurationIsPresent() {
        ChronoGraph graph = this.getGraph();
        assertNotNull(graph.getChronoGraphConfiguration());
    }

    @Test
    public void idExistenceCheckIsEnabledByDefault() {
        ChronoGraph graph = this.getGraph();
        assertTrue(graph.getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled());
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
    public void canDisableIdExistenceCheckWithTestAnnotation() {
        ChronoGraph graph = this.getGraph();
        assertFalse(graph.getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled());
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
    public void disablingIdExistenceCheckWorks() {
        ChronoGraph graph = this.getGraph();
        assertFalse(graph.getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled());
        graph.tx().open();
        Vertex v1 = graph.addVertex(T.id, "MyAwesomeId");
        graph.tx().commit();

        graph.tx().open();
        // this should now be okay
        Vertex v2 = graph.addVertex(T.id, "MyAwesomeId");
        assertThat(v2, is(notNullValue()));
    }

    @Test
    public void enablingIdExistenceCheckThrowsExceptionIfIdIsUsedTwice() {
        ChronoGraph graph = this.getGraph();
        assertTrue(graph.getChronoGraphConfiguration().isCheckIdExistenceOnAddEnabled());
        graph.tx().open();
        Vertex v1 = graph.addVertex(T.id, "MyAwesomeId");
        assertNotNull(v1);
        graph.tx().commit();
        graph.tx().open();
        try {
            graph.addVertex(T.id, "MyAwesomeId");
            fail("Managed to use the same ID twice!");
        } catch (IllegalArgumentException expected) {
            // pass
        }
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
    public void canDisableAutoStartTransactions() {
        ChronoGraph graph = this.getGraph();
        assertFalse(graph.getChronoGraphConfiguration().isTransactionAutoOpenEnabled());
        try {
            graph.addVertex();
            fail("Managed to add a vertex to a graph while auto-transactions are disabled!");
        } catch (IllegalStateException expected) {
            // pass
        }
        // try in another thread
        GraphAccessTestRunnable runnable = new GraphAccessTestRunnable(graph);
        Thread worker = new Thread(runnable);
        worker.start();
        try {
            worker.join();
        } catch (InterruptedException e) {
            // ignored
        }
        assertFalse(runnable.canAccessGraph());
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN, value = "false")
    public void canOpenManualTransactionsWhenAutoStartIsDisabled() {
        ChronoGraph graph = this.getGraph();
        assertFalse(graph.getChronoGraphConfiguration().isTransactionAutoOpenEnabled());
        graph.tx().open();
        try {
            Vertex v = graph.addVertex();
            v.property("name", "Martin");
            graph.tx().commit();
        } finally {
            if (graph.tx().isOpen()) {
                graph.tx().close();
            }
        }
    }

    @Test
    @DontRunWithBackend({InMemoryChronoDB.BACKEND_NAME})
    public void canStartUpChronoGraphInReadOnlyMode() {
        // get the graph in default configuration (let it set up its persistent stores, execute migration chains etc.)
        ChronoGraph graph = this.getGraph();
        assertNotNull(graph);

        // now that the graph has been established on persistent media, close it and reopen as read-only.
        Configuration additionalConfig = new BaseConfiguration();
        additionalConfig.setProperty(ChronoDBConfiguration.READONLY, true);
        ChronoGraph reopenedGraph = this.closeAndReopenGraph(additionalConfig);

        // this should be allowed...
        reopenedGraph.tx().open();
        long vertexCount = reopenedGraph.traversal().V().count().next();
        assertEquals(0L, vertexCount);
        reopenedGraph.tx().rollback();
        // but this should not be allowed... (read-only!)
        try {
            reopenedGraph.tx().open();
            reopenedGraph.addVertex(T.id, "123");
            reopenedGraph.tx().commit();
            fail("Managed to perform commit() on a read-only graph!");
        } catch (TransactionException expected) {
            // pass
        }
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private static class GraphAccessTestRunnable implements Runnable {

        private final ChronoGraph graph;
        private boolean canAccessGraph;

        public GraphAccessTestRunnable(ChronoGraph graph) {
            checkNotNull(graph, "Precondition violation - argument 'graph' must not be NULL!");
            this.graph = graph;
        }

        @Override
        public void run() {
            try {
                this.graph.addVertex();
                this.canAccessGraph = true;
            } catch (IllegalStateException expected) {
                this.canAccessGraph = false;
            } finally {
                if (this.graph.tx().isOpen()) {
                    this.graph.tx().rollback();
                }
            }
        }

        public boolean canAccessGraph() {
            return this.canAccessGraph;
        }
    }
}
