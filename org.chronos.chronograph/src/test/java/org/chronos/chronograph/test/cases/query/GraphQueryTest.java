package org.chronos.chronograph.test.cases.query;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.builder.query.CP;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.chronograph.test.util.ChronoGraphTestUtil;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class GraphQueryTest extends AllChronoGraphBackendsTest {

    @Test
    public void basicQuerySyntaxTest() {
        ChronoGraph g = this.getGraph();
        Set<Vertex> set = g.traversal().V().has("name", CP.containsIgnoreCase("EVA")).has("kind", "entity").toSet();
        assertNotNull(set);
        assertTrue(set.isEmpty());
    }

    @Test
    public void queryToGremlinSyntaxTest() {
        ChronoGraph g = this.getGraph();
        Set<Edge> set = g.traversal().V().has("name", CP.containsIgnoreCase("EVA")).outE().toSet();
        assertNotNull(set);
        assertTrue(set.isEmpty());
    }

    @Test
    public void simpleVertexQueriesOnPersistentStateWithoutIndexWork() {
        this.performSimpleVertexQueries(false, false, true);
    }

    @Test
    public void simpleVertexQueriesOnTransientStateWithoutIndexWork() {
        this.performSimpleVertexQueries(false, false, false);
    }

    @Test
    public void simpleVertexQueriesOnPersistentStateWithIndexWork() {
        this.performSimpleVertexQueries(true, true, true);
    }

    @Test
    public void simpleVertexQueriesOnTransientStateWithIndexWork() {
        this.performSimpleVertexQueries(true, true, false);
    }

    @Test
    public void gremlinQueriesOnPersistentStateWithoutIndexWork() {
        this.performVertexToGremlinQueries(false, false, true);
    }

    @Test
    public void gremlinQueriesOnTransientStateWithoutIndexWork() {
        this.performVertexToGremlinQueries(false, false, false);
    }

    @Test
    public void gremlinQueriesOnPersistentStateWithIndexWork() {
        this.performVertexToGremlinQueries(true, true, true);
    }

    @Test
    public void gremlinQueriesOnTransientStateWithIndexWork() {
        this.performVertexToGremlinQueries(true, true, false);
    }

    @Test
    public void canQueryNonStandardObjectTypeInHasClause() {
        ChronoGraph graph = this.getGraph();
        graph.addVertex("name", new FullName("John", "Doe"));
        graph.addVertex("name", new FullName("Jane", "Doe"));
        graph.tx().commit();

        assertEquals(1, graph.traversal().V().has("name", new FullName("John", "Doe")).toSet().size());
    }

    @Test
    public void canFindStringWithTrailingWhitespace() {
        ChronoGraph graph = this.getGraph();
        graph.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        graph.addVertex("name", "John ");
        graph.addVertex("name", "John");
        graph.tx().commit();

        assertEquals("John ",
            Iterables.getOnlyElement(graph.traversal().V().has("name", "John ").toSet()).value("name"));
    }

    @Test
    public void canFindStringWithLeadingWhitespace() {
        ChronoGraph graph = this.getGraph();
        graph.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        graph.addVertex("name", " John");
        graph.addVertex("name", "John");
        graph.tx().commit();

        assertEquals(" John",
            Iterables.getOnlyElement(graph.traversal().V().has("name", " John").toSet()).value("name"));
    }

    @Test
    public void indexQueriesAreLazyLoading(){

        // IMPORTANT: BE VERY VERY CAREFUL WHEN RUNNING THIS TEST IN THE DEBUGGER!
        // The problem is that java debuggers often call the "toString()" method on
        // the objects in scope. However, calling "toString()" on a lazy graph element
        // proxy leads to the resolution of the proxy. This test ASSERTS that the lazy
        // proxies have NOT been resolved yet.
        // In short, if this test works when you run it but FAILS when you are debugging
        // it, then it is most likely due to "toString()" called by the debugger resolving
        // the proxies. It is also not possible for us to change "toString()", because it
        // is specified by the TinkerPop standard.

        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onEdgeProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();
        Vertex a = g.addVertex(T.id, "a", "p", "1");
        Vertex b = g.addVertex(T.id, "b", "p", "2");
        Vertex c = g.addVertex(T.id, "c", "p", "1");
        Edge e1 = a.addEdge("l", b, "p", "1");
        Edge e2 = a.addEdge("l", c, "p", "2");
        Edge e3 = b.addEdge("l", a, "p", "1");
        Edge e4 = b.addEdge("l", c, "p", "2");
        Edge e5 = c.addEdge("l", a, "p", "1");
        Edge e6 = c.addEdge("l", b, "p", "2");
        Edge e7 = a.addEdge("l", a, "p", "1");
        Edge e8 = b.addEdge("l", b, "p", "2");
        Edge e9 = c.addEdge("l", c, "p", "1");
        g.tx().commit();

        Set<Vertex> vertices = g.traversal().V().has("p", "1").toSet();
        assertThat(vertices.size(), is(2));
        assertThat(vertices, containsInAnyOrder(a, c));
        vertices.forEach(v -> assertThat(ChronoGraphTestUtil.isFullyLoaded(v), is(false)));

        Set<Edge> edges = g.traversal().E().has("p", "1").toSet();
        assertThat(edges.size(), is(5));
        assertThat(edges, containsInAnyOrder(e1, e3, e5, e7, e9));
        edges.forEach(e -> assertThat(ChronoGraphTestUtil.isFullyLoaded(e), is(false)));

        assertFalse(g.tx().getCurrentTransaction().getContext().isDirty());
    }

    // =====================================================================================================================
    // HELPER METHODS
    // =====================================================================================================================

    private void performSimpleVertexQueries(final boolean indexName, final boolean indexAge,
                                            final boolean performCommit) {
        ChronoGraph g = this.getGraph();
        if (indexName) {
            g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
            g.tx().commit();
        }
        if (indexAge) {
            g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("age").acrossAllTimestamps().build();
            g.tx().commit();
        }
        Vertex vMartin = g.addVertex("name", "Martin", "age", 26);
        Vertex vJohn = g.addVertex("name", "John", "age", 19);
        Vertex vMaria = g.addVertex("name", "Maria", "age", 35);

        if (performCommit) {
            g.tx().commit();
        }

        Set<Vertex> set;

        set = g.traversal().V().has("name", CP.contains("n")).toSet(); // Marti[n] and Joh[n]
        assertNotNull(set);
        assertEquals(2, set.size());
        assertTrue(set.contains(vMartin));
        assertTrue(set.contains(vJohn));

        set = g.traversal().V().has("name", CP.containsIgnoreCase("N")).toSet(); // Marti[n] and Joh[n]
        assertNotNull(set);
        assertEquals(2, set.size());
        assertTrue(set.contains(vMartin));
        assertTrue(set.contains(vJohn));

        set = g.traversal().V().has("name", CP.contains("N")).toSet(); // no capital 'N' to be found in the test data
        assertNotNull(set);
        assertEquals(0, set.size());

        set = g.traversal().V().has("name", CP.startsWith("Mar")).toSet(); // [Mar]tin and [Mar]ia
        assertNotNull(set);
        assertEquals(2, set.size());
        assertTrue(set.contains(vMartin));
        assertTrue(set.contains(vMaria));

        set = g.traversal().V().has("name", CP.startsWithIgnoreCase("mar")).toSet(); // [Mar]tin and [Mar]ia
        assertNotNull(set);
        assertEquals(2, set.size());
        assertTrue(set.contains(vMartin));
        assertTrue(set.contains(vMaria));

        set = g.traversal().V().has("name", CP.notContains("t")).toSet(); // all except Mar[t]in
        assertNotNull(set);
        assertEquals(2, set.size());
        assertTrue(set.contains(vJohn));
        assertTrue(set.contains(vMaria));

        set = g.traversal().V().has("name", CP.matchesRegex(".*r.*i.*")).toSet(); // Ma[r]t[i]n and Ma[r][i]a
        assertNotNull(set);
        assertEquals(2, set.size());
        assertTrue(set.contains(vMartin));
        assertTrue(set.contains(vMaria));

        set = g.traversal().V().has("name", CP.matchesRegex("(?i).*R.*I.*")).toSet(); // Ma[r]t[i]n and Ma[r][i]a
        assertNotNull(set);
        assertEquals(2, set.size());
        assertTrue(set.contains(vMartin));
        assertTrue(set.contains(vMaria));
    }

    private void performVertexToGremlinQueries(final boolean indexName, final boolean indexAge,
                                               final boolean performCommit) {
        ChronoGraph g = this.getGraph();
        if (indexName) {
            g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
            g.tx().commit();
        }
        if (indexAge) {
            g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("age").acrossAllTimestamps().build();
            g.tx().commit();
        }
        Vertex vMartin = g.addVertex("name", "Martin", "age", 26);
        Vertex vJohn = g.addVertex("name", "John", "age", 19);
        Vertex vMaria = g.addVertex("name", "Maria", "age", 35);

        vMartin.addEdge("knows", vJohn);
        vJohn.addEdge("knows", vMaria);
        vMaria.addEdge("knows", vMartin);

        if (performCommit) {
            g.tx().commit();
        }
        Set<Vertex> set;
        // - The first query (name ends with 'N') delivers Martin and John.
        // - Traversing the 'knows' edge on both Martin and John leads to:
        // -- John (Martin-knows->John)
        // -- Maria (John-knows->Maria)
        set = g.traversal().V().has("name", CP.endsWithIgnoreCase("N")).out("knows").toSet();
        assertNotNull(set);
        assertEquals(2, set.size());
        assertTrue(set.contains(vJohn));
        assertTrue(set.contains(vMaria));
    }

    private static class FullName {

        private String firstName;
        private String lastName;

        @SuppressWarnings("unused")
        protected FullName() {
            // (de-)serialization constructor
        }

        public FullName(final String firstName, final String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (this.firstName == null ? 0 : this.firstName.hashCode());
            result = prime * result + (this.lastName == null ? 0 : this.lastName.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (this.getClass() != obj.getClass()) {
                return false;
            }
            FullName other = (FullName) obj;
            if (this.firstName == null) {
                if (other.firstName != null) {
                    return false;
                }
            } else if (!this.firstName.equals(other.firstName)) {
                return false;
            }
            if (this.lastName == null) {
                if (other.lastName != null) {
                    return false;
                }
            } else if (!this.lastName.equals(other.lastName)) {
                return false;
            }
            return true;
        }

    }

}
