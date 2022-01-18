package org.chronos.chronograph.test.cases.gremlin;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.builder.query.CP;
import org.chronos.chronograph.api.index.ChronoGraphIndexManager;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class GremlinTest extends AllChronoGraphBackendsTest {

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.PERFORMANCE_LOGGING_FOR_COMMITS, value = "true")
    public void basicVertexRetrievalWorks() {
        ChronoGraph graph = this.getGraph();
        graph.addVertex("name", "Martin", "age", 26);
        graph.addVertex("name", "Martin", "age", 10);
        graph.addVertex("name", "John", "age", 26);
        graph.tx().commit();
        Set<Vertex> vertices = graph.traversal().V().has("name", "Martin").has("age", 26).toSet();
        assertEquals(1, vertices.size());
        Vertex vertex = Iterables.getOnlyElement(vertices);
        assertEquals("Martin", vertex.value("name"));
        assertEquals(26, (int) vertex.value("age"));
    }

    @Test
    public void basicEgdeRetrievalWorks() {
        ChronoGraph graph = this.getGraph();
        Vertex me = graph.addVertex("kind", "person", "name", "Martin");
        Vertex chronos = graph.addVertex("kind", "project", "name", "Chronos");
        Vertex otherProject = graph.addVertex("kind", "project", "name", "Other Project");
        me.addEdge("worksOn", chronos, "since", "2015-07-30");
        me.addEdge("worksOn", otherProject, "since", "2000-01-01");
        graph.tx().commit();
        Set<Edge> edges = graph.traversal().E().has("since", "2015-07-30").toSet();
        assertEquals(1, edges.size());
    }

    @Test
    public void gremlinNavigationWorks() {
        ChronoGraph graph = this.getGraph();
        Vertex me = graph.addVertex("kind", "person", "name", "Martin");
        Vertex chronos = graph.addVertex("kind", "project", "name", "Chronos");
        Vertex otherProject = graph.addVertex("kind", "project", "name", "Other Project");
        me.addEdge("worksOn", chronos);
        me.addEdge("worksOn", otherProject);
        graph.tx().commit();
        Set<Vertex> vertices = graph.traversal().V().has("kind", "person").has("name", "Martin").out("worksOn")
            .has("name", "Chronos").toSet();
        assertEquals(1, vertices.size());
        Vertex vertex = Iterables.getOnlyElement(vertices);
        assertEquals("Chronos", vertex.value("name"));
        assertEquals("project", vertex.value("kind"));
    }

    @Test
    public void multipleHasClausesWorksOnIndexedVertexProperty() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphIndexManager indexManager = graph.getIndexManagerOnMaster();
        indexManager.create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        indexManager.create().stringIndex().onVertexProperty("kind").acrossAllTimestamps().build();
        indexManager.reindexAll();
        Vertex vExpected = graph.addVertex("kind", "person", "name", "Martin");
        graph.addVertex("kind", "person", "name", "Thomas");
        graph.addVertex("kind", "project", "name", "Chronos");
        graph.addVertex("kind", "project", "name", "Martin");
        // multiple HAS clauses need to be AND-connected.
        Set<Vertex> result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(vExpected));
        graph.tx().commit();
        result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(vExpected));
    }

    @Test
    public void multipleHasClausesWorksOnNonIndexedVertexProperty() {
        ChronoGraph graph = this.getGraph();
        Vertex vExpected = graph.addVertex("kind", "person", "name", "Martin");
        graph.addVertex("kind", "person", "name", "Thomas");
        graph.addVertex("kind", "project", "name", "Chronos");
        graph.addVertex("kind", "project", "name", "Martin");
        // multiple HAS clauses need to be AND-connected.
        Set<Vertex> result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(vExpected));
        graph.tx().commit();
        result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(vExpected));
    }

    @Test
    public void multipleHasClausesWorksOnMixedIndexedVertexProperty() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphIndexManager indexManager = graph.getIndexManagerOnMaster();
        // do not index 'name', but index 'kind'
        indexManager.create().stringIndex().onVertexProperty("kind").acrossAllTimestamps().build();
        indexManager.reindexAll();
        Vertex vExpected = graph.addVertex("kind", "person", "name", "Martin");
        graph.addVertex("kind", "person", "name", "Thomas");
        graph.addVertex("kind", "project", "name", "Chronos");
        graph.addVertex("kind", "project", "name", "Martin");
        // multiple HAS clauses need to be AND-connected.
        Set<Vertex> result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(vExpected));
        graph.tx().commit();
        result = graph.traversal().V().has("name", "Martin").has("kind", "person").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(vExpected));
    }

    @Test
    public void multipleHasClausesWorksOnIndexedEdgeProperty() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphIndexManager indexManager = graph.getIndexManagerOnMaster();
        indexManager.create().stringIndex().onEdgeProperty("a").acrossAllTimestamps().build();
        indexManager.create().stringIndex().onEdgeProperty("b").acrossAllTimestamps().build();
        indexManager.reindexAll();
        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Edge eExpected = v1.addEdge("test1", v2, "a", "yes", "b", "true");
        v1.addEdge("test2", v2, "a", "no", "b", "true");
        v1.addEdge("test3", v2, "a", "yes", "b", "false");
        v1.addEdge("test4", v2, "a", "no", "b", "false");

        // multiple HAS clauses need to be AND-connected.
        Set<Edge> result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(eExpected));
        graph.tx().commit();
        result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(eExpected));
    }

    @Test
    public void multipleHasClausesWorksOnMixedEdgeProperty() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphIndexManager indexManager = graph.getIndexManagerOnMaster();
        // do not index 'a', but index 'b'
        indexManager.create().stringIndex().onEdgeProperty("b").acrossAllTimestamps().build();
        indexManager.reindexAll();
        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Edge eExpected = v1.addEdge("test1", v2, "a", "yes", "b", "true");
        v1.addEdge("test2", v2, "a", "no", "b", "true");
        v1.addEdge("test3", v2, "a", "yes", "b", "false");
        v1.addEdge("test4", v2, "a", "no", "b", "false");

        // multiple HAS clauses need to be AND-connected.
        Set<Edge> result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(eExpected));
        graph.tx().commit();
        result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(eExpected));
    }

    @Test
    public void multipleHasClausesWorksOnNonIndexedEdgeProperty() {
        ChronoGraph graph = this.getGraph();
        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Edge eExpected = v1.addEdge("test1", v2, "a", "yes", "b", "true");
        v1.addEdge("test2", v2, "a", "no", "b", "true");
        v1.addEdge("test3", v2, "a", "yes", "b", "false");
        v1.addEdge("test4", v2, "a", "no", "b", "false");

        // multiple HAS clauses need to be AND-connected.
        Set<Edge> result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(eExpected));
        graph.tx().commit();
        result = graph.traversal().E().has("a", "yes").has("b", "true").toSet();
        assertEquals(1, result.size());
        assertTrue(result.contains(eExpected));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void usingSubqueriesWorks() {
        ChronoGraph graph = this.getGraph();
        Vertex vMother = graph.addVertex("name", "Eva");
        Vertex vFather = graph.addVertex("name", "Adam");
        Vertex vSon1 = graph.addVertex("name", "Kain");
        Vertex vSon2 = graph.addVertex("name", "Abel");
        Vertex vDaughter = graph.addVertex("name", "Sarah");
        vMother.addEdge("married", vFather);
        vFather.addEdge("married", vMother);
        vMother.addEdge("son", vSon1);
        vMother.addEdge("son", vSon2);
        vMother.addEdge("daughter", vDaughter);
        vFather.addEdge("son", vSon1);
        vFather.addEdge("son", vSon2);
        vFather.addEdge("daughter", vDaughter);
        Set<Vertex> vertices = graph.traversal().V(vMother).union(__.out("son"), __.out("daughter")).toSet();
        assertEquals(Sets.newHashSet(vSon1, vSon2, vDaughter), vertices);
    }

    @Test
    public void canExecuteGraphEHasLabel() {
        ChronoGraph graph = this.getGraph();
        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        Vertex v3 = graph.addVertex();
        v1.addEdge("forward", v2);
        v2.addEdge("forward", v3);
        v3.addEdge("forward", v1);
        v1.addEdge("backward", v3);
        v2.addEdge("backward", v2);
        v3.addEdge("backward", v2);
        assertEquals(3, Iterators.size(graph.traversal().E().hasLabel("forward")));
        assertEquals(3, Iterators.size(graph.traversal().E().hasLabel("backward")));
        graph.tx().commit();
        assertEquals(3, Iterators.size(graph.traversal().E().hasLabel("forward")));
        assertEquals(3, Iterators.size(graph.traversal().E().hasLabel("backward")));
    }

    @Test
    public void canExecuteGraphVHasLabel() {
        ChronoGraph graph = this.getGraph();
        graph.addVertex(T.label, "Person");
        graph.addVertex(T.label, "Person");
        graph.addVertex(T.label, "Location");
        assertEquals(2, Iterators.size(graph.traversal().V().hasLabel("Person")));
        assertEquals(1, Iterators.size(graph.traversal().V().hasLabel("Location")));
        graph.tx().commit();
        assertEquals(2, Iterators.size(graph.traversal().V().hasLabel("Person")));
        assertEquals(1, Iterators.size(graph.traversal().V().hasLabel("Location")));

    }

    @Test
    public void canExecuteGraphIndexQueriesWithStringPredicatesWithoutIndices(){
        ChronoGraph graph = this.getGraph();
        graph.addVertex(T.label, "Person", "firstname", "John", "lastname", "Doe");
        graph.addVertex(T.label, "Person", "firstname", "Jane", "lastname", "Doe");
        graph.addVertex(T.label, "Person", "firstname", "Jack", "lastname", "Smith");
        graph.addVertex(T.label, "Person", "firstname", "Sarah", "lastname", "Doe");
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.startsWith("J")).values("firstname").toSet(), containsInAnyOrder("John","Jack", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.startsWithIgnoreCase("ja")).values("firstname").toSet(), containsInAnyOrder("Jack", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notStartsWith("J")).values("firstname").toSet(), containsInAnyOrder("Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notStartsWithIgnoreCase("ja")).values("firstname").toSet(), containsInAnyOrder("John", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.endsWith("n")).values("firstname").toSet(), containsInAnyOrder("John"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.endsWithIgnoreCase("K")).values("firstname").toSet(), containsInAnyOrder("Jack"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notEndsWith("n")).values("firstname").toSet(), containsInAnyOrder("Jane", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notEndsWithIgnoreCase("K")).values("firstname").toSet(), containsInAnyOrder("John", "Jane", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.contains("a")).values("firstname").toSet(), containsInAnyOrder("Jane", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notContains("a")).values("firstname").toSet(), containsInAnyOrder("John"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.containsIgnoreCase("AN")).values("firstname").toSet(), containsInAnyOrder("Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notContainsIgnoreCase("AN")).values("firstname").toSet(), containsInAnyOrder("John", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.matchesRegex("J.*n.*")).values("firstname").toSet(), containsInAnyOrder("John", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notMatchesRegex("J.*n.*")).values("firstname").toSet(), containsInAnyOrder("Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.matchesRegexIgnoreCase("j.*n.*")).values("firstname").toSet(), containsInAnyOrder("John", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notMatchesRegexIgnoreCase("j.*n.*")).values("firstname").toSet(), containsInAnyOrder("Jack", "Sarah"));

        graph.tx().commit();
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.startsWith("J")).values("firstname").toSet(), containsInAnyOrder("John","Jack", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.startsWithIgnoreCase("ja")).values("firstname").toSet(), containsInAnyOrder("Jack", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notStartsWith("J")).values("firstname").toSet(), containsInAnyOrder("Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notStartsWithIgnoreCase("ja")).values("firstname").toSet(), containsInAnyOrder("John", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.endsWith("n")).values("firstname").toSet(), containsInAnyOrder("John"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.endsWithIgnoreCase("K")).values("firstname").toSet(), containsInAnyOrder("Jack"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notEndsWith("n")).values("firstname").toSet(), containsInAnyOrder("Jane", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notEndsWithIgnoreCase("K")).values("firstname").toSet(), containsInAnyOrder("John", "Jane", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.contains("a")).values("firstname").toSet(), containsInAnyOrder("Jane", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notContains("a")).values("firstname").toSet(), containsInAnyOrder("John"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.containsIgnoreCase("AN")).values("firstname").toSet(), containsInAnyOrder("Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notContainsIgnoreCase("AN")).values("firstname").toSet(), containsInAnyOrder("John", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.matchesRegex("J.*n.*")).values("firstname").toSet(), containsInAnyOrder("John", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notMatchesRegex("J.*n.*")).values("firstname").toSet(), containsInAnyOrder("Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.matchesRegexIgnoreCase("j.*n.*")).values("firstname").toSet(), containsInAnyOrder("John", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notMatchesRegexIgnoreCase("j.*n.*")).values("firstname").toSet(), containsInAnyOrder("Jack", "Sarah"));
    }

    @Test
    public void canExecuteGraphIndexQueriesWithStringPredicatesWithIndices(){
        ChronoGraph graph = this.getGraph();
        graph.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("firstname").acrossAllTimestamps().build();
        graph.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("lastname").acrossAllTimestamps().build();
        graph.getIndexManagerOnMaster().reindexAll();
        graph.addVertex(T.label, "Person", "firstname", "John", "lastname", "Doe");
        graph.addVertex(T.label, "Person", "firstname", "Jane", "lastname", "Doe");
        graph.addVertex(T.label, "Person", "firstname", "Jack", "lastname", "Smith");
        graph.addVertex(T.label, "Person", "firstname", "Sarah", "lastname", "Doe");
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.startsWith("J")).values("firstname").toSet(), containsInAnyOrder("John","Jack", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.startsWithIgnoreCase("ja")).values("firstname").toSet(), containsInAnyOrder("Jack", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notStartsWith("J")).values("firstname").toSet(), containsInAnyOrder("Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notStartsWithIgnoreCase("ja")).values("firstname").toSet(), containsInAnyOrder("John", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.endsWith("n")).values("firstname").toSet(), containsInAnyOrder("John"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.endsWithIgnoreCase("K")).values("firstname").toSet(), containsInAnyOrder("Jack"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notEndsWith("n")).values("firstname").toSet(), containsInAnyOrder("Jane", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notEndsWithIgnoreCase("K")).values("firstname").toSet(), containsInAnyOrder("John", "Jane", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.contains("a")).values("firstname").toSet(), containsInAnyOrder("Jane", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notContains("a")).values("firstname").toSet(), containsInAnyOrder("John"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.containsIgnoreCase("AN")).values("firstname").toSet(), containsInAnyOrder("Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notContainsIgnoreCase("AN")).values("firstname").toSet(), containsInAnyOrder("John", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.matchesRegex("J.*n.*")).values("firstname").toSet(), containsInAnyOrder("John", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notMatchesRegex("J.*n.*")).values("firstname").toSet(), containsInAnyOrder("Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.matchesRegexIgnoreCase("j.*n.*")).values("firstname").toSet(), containsInAnyOrder("John", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notMatchesRegexIgnoreCase("j.*n.*")).values("firstname").toSet(), containsInAnyOrder("Jack", "Sarah"));

        graph.tx().commit();
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.startsWith("J")).values("firstname").toSet(), containsInAnyOrder("John","Jack", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.startsWithIgnoreCase("ja")).values("firstname").toSet(), containsInAnyOrder("Jack", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notStartsWith("J")).values("firstname").toSet(), containsInAnyOrder("Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notStartsWithIgnoreCase("ja")).values("firstname").toSet(), containsInAnyOrder("John", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.endsWith("n")).values("firstname").toSet(), containsInAnyOrder("John"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.endsWithIgnoreCase("K")).values("firstname").toSet(), containsInAnyOrder("Jack"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notEndsWith("n")).values("firstname").toSet(), containsInAnyOrder("Jane", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notEndsWithIgnoreCase("K")).values("firstname").toSet(), containsInAnyOrder("John", "Jane", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.contains("a")).values("firstname").toSet(), containsInAnyOrder("Jane", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notContains("a")).values("firstname").toSet(), containsInAnyOrder("John"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.containsIgnoreCase("AN")).values("firstname").toSet(), containsInAnyOrder("Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notContainsIgnoreCase("AN")).values("firstname").toSet(), containsInAnyOrder("John", "Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.matchesRegex("J.*n.*")).values("firstname").toSet(), containsInAnyOrder("John", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notMatchesRegex("J.*n.*")).values("firstname").toSet(), containsInAnyOrder("Jack", "Sarah"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.matchesRegexIgnoreCase("j.*n.*")).values("firstname").toSet(), containsInAnyOrder("John", "Jane"));
        MatcherAssert.assertThat(graph.traversal().V().has("firstname", CP.notMatchesRegexIgnoreCase("j.*n.*")).values("firstname").toSet(), containsInAnyOrder("Jack", "Sarah"));
    }
}
