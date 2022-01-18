package org.chronos.chronograph.test.cases.transaction;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class ElementIdReuseTest extends AllChronoGraphBackendsTest {


    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "true")
    public void canCreateDeleteAndReuseEdgeIdInSameTransaction() {
        ChronoGraph graph = this.getGraph();
        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Vertex v3 = graph.addVertex();
        // create an edge with a fixed ID
        Edge e1 = v1.addEdge("test", v2, T.id, "1");
        assertEquals(1, Iterators.size(graph.edges("1")));
        e1.property("hello", "world");
        // delete the edge
        e1.remove();
        // assert that it's gone
        assertEquals(0, Iterators.size(graph.edges("1")));
        // add another edge with the same ID
        Edge e2 = v1.addEdge("test", v3, T.id, "1");
        assertNotNull(e2);
        e2.property("foo", "bar");
        Edge edge = Iterators.getOnlyElement(graph.edges("1"));
        assertEquals(null, edge.property("hello").orElse(null));
        assertEquals("bar", edge.property("foo").orElse(null));
    }

    @Test
    public void canDeleteAndReuseEdgeIdBetweenDifferentVerticesInSameTransaction(){
        ChronoGraph graph = this.getGraph();
        Vertex v1 = graph.addVertex(T.id, "v1");
        Vertex v2 = graph.addVertex(T.id, "v2");
        Vertex v3 = graph.addVertex(T.id, "v3");
        // create an edge with a fixed ID
        Edge e1 = v1.addEdge("test", v2, T.id, "1");
        assertEquals(1, Iterators.size(graph.edges("1")));
        e1.property("hello", "world");
        graph.tx().commit();

        // delete the edge
        e1.remove();
        Edge e2  = v3.addEdge("test", v2, T.id, "1");

        assertEquals(v3.id(), e2.outVertex().id());
        assertEquals(v2.id(), e2.inVertex().id());

        graph.tx().commit();

        Edge edgeWithReusedId = Iterables.getOnlyElement(graph.traversal().E("1").toSet());
        assertEquals(v3.id(), edgeWithReusedId.outVertex().id());
        assertEquals(v2.id(), edgeWithReusedId.inVertex().id());
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "true")
    public void canDeleteAndOverridePersistentElement() {
        ChronoGraph g = this.getGraph();
        Vertex v1 = g.addVertex();
        Vertex v2 = g.addVertex();
        Vertex v3 = g.addVertex();
        Edge e1 = v1.addEdge("test", v2, T.id, "1");
        assertNotNull(e1);
        g.tx().commit();

        e1.remove();
        Edge e2 = v1.addEdge("test", v3, T.id, "1");
        assertNotNull(e2);
        g.tx().commit();
    }
}
