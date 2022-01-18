package org.chronos.chronograph.test.cases.transaction;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

import static org.junit.Assert.*;

public class TransactionCloseTest extends AllChronoGraphBackendsTest {

    @Test
    public void transactionErrorMessageReportsGraphStatus() {
        ChronoGraph graph = this.getGraph();

        graph.tx().open();
        Vertex v = graph.addVertex("my-id");
        graph.close();
        try {
            v.property("name", "My Vertex");
            fail("Managed to operate on a vertex with the transaction already closed!");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("This ChronoGraph instance has already been closed"));
        }
    }

    @Test
    public void threadedTransactionErrorMessageReportsGraphStatus1() {
        ChronoGraph graph = this.getGraph();

        try (ChronoGraph txGraph = graph.tx().createThreadedTx()) {
            Vertex v = txGraph.addVertex("my-id");
            txGraph.close();
            v.property("name", "My Vertex");
            fail("Managed to operate on a vertex with the transaction already closed!");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("The ChronoGraph instance is still open"));
        }
    }


    @Test
    public void threadedTransactionErrorMessageReportsGraphStatus2() {
        ChronoGraph graph = this.getGraph();

        try (ChronoGraph txGraph = graph.tx().createThreadedTx()) {
            Vertex v = txGraph.addVertex("my-id");
            graph.close();
            v.property("name", "My Vertex");
            fail("Managed to operate on a vertex with the transaction already closed!");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("The ChronoGraph instance has also been closed"));
        }
    }

}
