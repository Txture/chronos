package org.chronos.chronograph.test.cases.structure;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.common.test.utils.TestUtils;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.structure.ElementLifecycleStatus;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class GraphIntegrityTortureTest extends AllChronoGraphBackendsTest {

    // =================================================================================================================
    // TESTS
    // =================================================================================================================

    @Test
    public void canAddAndRemoveEdgesWithCustomIDs() {
        ChronoGraph g = this.getGraph();
        g.tx().open();
        int vertexCount = 10;
        int edgeCount = 30;
        int iterations = 10;
        Map<Integer, Vertex> vertexById = Maps.newHashMap();
        for (int i = 0; i < vertexCount; i++) {
            vertexById.put(i, g.addVertex(T.id, String.valueOf(i)));
        }
        g.tx().commit();
        // start the test
        for (int iteration = 0; iteration < iterations; iteration++) {
            System.out.println("Iteration #" + (iteration + 1));
            g.tx().open();
            Set<PlannedEdge> plannedEdges = Sets.newHashSet();
            for (int e = 0; e < edgeCount; e++) {
                plannedEdges.add(new PlannedEdge(
                    // from vertex #
                    TestUtils.randomBetween(0, vertexCount - 1),
                    // to vertex #
                    TestUtils.randomBetween(0, vertexCount - 1),
                    // edge ID
                    e)
                );
            }
            g.edges().forEachRemaining(Edge::remove);
            for (PlannedEdge plannedEdge : plannedEdges) {
                Vertex source = Iterators.getOnlyElement(g.vertices(String.valueOf(plannedEdge.getFromId())));
                Vertex target = Iterators.getOnlyElement(g.vertices(String.valueOf(plannedEdge.getToId())));
                source.addEdge("test", target, T.id, String.valueOf(plannedEdge.edgeId));
            }
            g.tx().commit();
        }
    }

    @Test
    public void cannotAddEdgeToRemovedVertex() {
        ChronoGraph g = this.getGraph();
        g.addVertex(T.id, "1");
        g.tx().commit();

        g.tx().open();
        Vertex v = Iterators.getOnlyElement(g.vertices("1"));
        v.remove();
        try {
            v.addEdge("test", v);
            fail("Managed to add edge to removed vertex!");
        } catch (IllegalStateException expected) {
            // pass
        }
    }

    @Test
    public void rollbackTest() {
        ChronoGraph g = this.getGraph();

        g.tx().open();

        g.addVertex(T.id, "123");
        g.tx().rollback();

        assertThat(g.tx().isOpen(), is(false));

        assertThat(g.vertices("123").hasNext(), is(false));

        g.tx().commit();

        g.tx().open();
        assertThat(g.vertices("123").hasNext(), is(false));
    }

    @Test
    public void selfEdgeTest() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        Vertex v = g.addVertex();
        Edge e = v.addEdge("self", v, T.id, "1");
        assertThat(((ChronoEdge) e).getStatus(), is(ElementLifecycleStatus.NEW));
        g.tx().commit();

        g.tx().open();
        assertThat(((ChronoEdge) e).getStatus(), is(ElementLifecycleStatus.PERSISTED));
        // remove the self-edge
        e.remove();
        assertThat(((ChronoEdge) e).getStatus(), is(ElementLifecycleStatus.REMOVED));
        // add it again (same ID!)
        e = v.addEdge("self", v, T.id, "1");
        assertThat(((ChronoEdge) e).getStatus(), is(ElementLifecycleStatus.PROPERTY_CHANGED));
        // remove it again
        e.remove();
        assertThat(((ChronoEdge) e).getStatus(), is(ElementLifecycleStatus.REMOVED));
        g.tx().commit();

        g.tx().open();

        // assert that it's gone
        assertThat(Iterators.size(g.edges()), is(0));
    }

    @Test
    public void edgeRecreationDropsProperties() {
        ChronoGraph g = this.getGraph();
        g.tx().open();
        Vertex v = g.addVertex();
        Edge e = v.addEdge("self", v, T.id, "1");
        e.property("foo", "bar");
        g.tx().commit();

        g.tx().open();
        e.remove();
        e = v.addEdge("self", v, T.id, "1");
        assertThat(e.property("foo").isPresent(), is(false));
        assertThat(g.edges("1").next().property("foo").isPresent(), is(false));
        assertThat(((ChronoEdge) g.edges("1").next()).getStatus(), is(ElementLifecycleStatus.PROPERTY_CHANGED));
        e.remove();
        assertThat(g.edges("1").hasNext(), is(false));
        g.tx().commit();

        g.tx().open();

        // assert that it's gone
        assertThat(Iterators.size(g.edges()), is(0));
    }

    @Test
    public void canAddAndRemoveVertexWithinSameTransaction() {
        ChronoGraph graph = this.getGraph();

        graph.tx().open();
        Vertex v = graph.addVertex(T.id, "1");
        graph.tx().commit();

        graph.tx().open();
        assertThat(((ChronoVertex) v).getStatus(), is(ElementLifecycleStatus.PERSISTED));
        v.remove();
        assertThat(((ChronoVertex) v).getStatus(), is(ElementLifecycleStatus.REMOVED));
        v = graph.addVertex(T.id, "1");
        assertThat(((ChronoVertex) v).getStatus(), is(ElementLifecycleStatus.PROPERTY_CHANGED));
        v.remove();
        assertThat(((ChronoVertex) v).getStatus(), is(ElementLifecycleStatus.REMOVED));
        graph.tx().commit();

        graph.tx().open();
        assertThat(Iterators.size(graph.vertices()), is(0));
    }

    @Test
    public void edgeProxyRebindingWorks() {
        ChronoGraph graph = this.getGraph();

        graph.tx().open();
        Vertex v = graph.addVertex();
        Edge e = v.addEdge("test", v, T.id, "1");
        graph.tx().commit();

        graph.tx().open();
        assertThat(((ChronoEdge) e).isRemoved(), is(false));
        e.property("Hello", "World");
        assertThat(e.property("Hello").orElse(null), is("World"));
        e.remove();
        assertThat(((ChronoEdge) e).isRemoved(), is(true));

        Vertex v2 = graph.addVertex();
        Edge e2 = v2.addEdge("test2", v2, T.id, "1");
        e2.property("foo", "bar");
        // note that, by adding e2 with the same ID as the deleted e1,
        // we re-bind the edge proxy to the new element
        assertThat(e, is(sameInstance(e2)));
        assertThat(e.property("Hello").isPresent(), is(false));
        assertThat(e.property("foo").orElse(null), is("bar"));
        graph.tx().commit();

        List<Edge> edges = Lists.newArrayList(graph.edges());
        assertThat(edges, contains(e));
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private static class PlannedEdge {

        private final int fromId;
        private final int toId;
        private final int edgeId;

        public PlannedEdge(int fromId, int toId, int edgeId) {
            this.fromId = fromId;
            this.toId = toId;
            this.edgeId = edgeId;
        }

        public int getFromId() {
            return this.fromId;
        }

        public int getToId() {
            return this.toId;
        }

        public int getEdgeId() {
            return this.edgeId;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("PlannedEdge{");
            sb.append("fromId=").append(this.fromId);
            sb.append(", toId=").append(this.toId);
            sb.append(", edgeId=").append(this.edgeId);
            sb.append('}');
            return sb.toString();
        }
    }

}
