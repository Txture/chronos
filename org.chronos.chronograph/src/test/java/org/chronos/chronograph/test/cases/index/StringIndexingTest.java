package org.chronos.chronograph.test.cases.index;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.builder.query.CP;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.chronos.common.test.junit.categories.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class StringIndexingTest extends AllChronoGraphBackendsTest {

    @Test
    public void indexCreationWorks() {
        ChronoGraph g = this.getGraph();
        ChronoGraphIndex index = g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
        assertNotNull(index);
        g.getIndexManager().reindexAll();
        g.tx().commit();

        Vertex v = g.addVertex("name", "Martin");
        g.addVertex("name", "John");
        g.addVertex("name", "Martina");

        g.tx().commit();

        // find by case-sensitive search
        assertEquals(v, Iterables.getOnlyElement(g.traversal().V().has("name", "Martin").toSet()));

    }

    @Test
    public void canDropIndex() {
        ChronoGraph g = this.getGraph();
        ChronoGraphIndex index = g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
        assertNotNull(index);
        g.getIndexManager().reindexAll();

        assertTrue(g.getIndexManager().isVertexPropertyIndexed("name"));

        g.getIndexManager().dropIndex(index);

        assertFalse(g.getIndexManager().isVertexPropertyIndexed("name"));
    }

    @Test
    public void canCreateMultipleIndices() {
        ChronoGraph g = this.getGraph();
        ChronoGraphIndex index = g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
        assertNotNull(index);
        g.getIndexManager().reindexAll();

        Vertex v = g.addVertex("name", "Martin");
        Vertex vJohn = g.addVertex("name", "John", "description", "foo");
        g.addVertex("name", "Martina", "description", "bar");

        g.tx().commit();

        assertTrue(g.getIndexManager().isVertexPropertyIndexed("name"));
        // find by case-sensitive search
        assertEquals(v, Iterables.getOnlyElement(g.traversal().V().has("name", "Martin").toSet()));

        ChronoGraphIndex descriptionIndex = g.getIndexManager().create().stringIndex().onVertexProperty("description").build();
        assertNotNull(descriptionIndex);
        g.tx().commit();

        assertTrue(g.getIndexManager().isVertexPropertyIndexed("name"));
        assertTrue(g.getIndexManager().isVertexPropertyIndexed("description"));
        assertEquals(v, Iterables.getOnlyElement(g.traversal().V().has("name", "Martin").toSet()));
        assertEquals(vJohn, Iterables.getOnlyElement(g.traversal().V().has("description", "foo").toSet()));

    }

    @Test
    public void renamingElementWorks() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
        g.getIndexManager().reindexAll();

        Vertex v1 = g.addVertex("name", "Test");
        Vertex v2 = g.addVertex("name", "John");
        g.tx().commit();

        // assert that we can find the elements
        assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Test").toSet()));
        assertEquals(v2, Iterables.getOnlyElement(g.traversal().V().has("name", "John").toSet()));

        // rename one element
        v1.property("name", "Hello");

        // assert that we can find the element by its new name
        assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Hello").toSet()));
        // assert that we cannot find the element by its old name anymore
        assertEquals(0, g.traversal().V().has("name", "Test").toSet().size());

        // perform the commit
        g.tx().commit();

        // assert that we can find the element by its new name
        assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Hello").toSet()));
        // assert that we cannot find the element by its old name anymore
        assertEquals(0, g.traversal().V().has("name", "Test").toSet().size());

    }

    @Test
    public void renamingElementInIncrementalCommitWorks() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
        g.getIndexManager().reindexAll();

        Vertex v1 = g.addVertex("name", "Test");
        Vertex v2 = g.addVertex("name", "John");
        g.tx().commit();

        // assert that we can find the elements
        assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Test").toSet()));
        assertEquals(v2, Iterables.getOnlyElement(g.traversal().V().has("name", "John").toSet()));

        g.tx().commitIncremental();

        // rename one element
        v1.property("name", "Hello");

        // assert that we can find the element by its new name
        assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Hello").toSet()));
        // assert that we cannot find the element by its old name anymore
        assertEquals(0, g.traversal().V().has("name", "Test").toSet().size());

        g.tx().commitIncremental();

        // perform the commit
        g.tx().commit();

        // assert that we can find the element by its new name
        assertEquals(v1, Iterables.getOnlyElement(g.traversal().V().has("name", "Hello").toSet()));
        // assert that we cannot find the element by its old name anymore
        assertEquals(0, g.traversal().V().has("name", "Test").toSet().size());
    }

    @Test
    @SuppressWarnings("unused")
    public void queryingTransientStateWithIndexWorks() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("color").build();
        g.getIndexManager().reindexAll();
        g.tx().open();
        Vertex v0 = g.addVertex("color", "red");
        Vertex v1 = g.addVertex("color", "green");
        Vertex v2 = g.addVertex("color", "blue");

        this.assertCommitAssert(() -> {
            assertEquals(Sets.newHashSet(v0, v1), g.traversal().V().has("color", CP.contains("r")).toSet());
        });
    }

    @Test
    @SuppressWarnings("unused")
    public void queryingTransientStateWithoutIndexWorks() {
        ChronoGraph g = this.getGraph();
        g.tx().open();
        Vertex v0 = g.addVertex("color", "red");
        Vertex v1 = g.addVertex("color", "green");
        Vertex v2 = g.addVertex("color", "blue");

        this.assertCommitAssert(() -> {
            assertEquals(Sets.newHashSet(v0, v1), g.traversal().V().has("color", CP.contains("r")).toSet());
        });
    }

    @Test
    @SuppressWarnings("unused")
    public void indexingOfMultiplicityManyValuesWorks() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("colors").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        Vertex v0 = g.addVertex("colors", Sets.newHashSet("red", "green", "blue"));
        Vertex v1 = g.addVertex("colors", Sets.newHashSet("red", "black", "white"));
        Vertex v2 = g.addVertex("colors", Sets.newHashSet("yellow", "green", "white"));

        // try to find all white vertices
        this.assertCommitAssert(() -> {
            assertEquals(Sets.newHashSet(v1, v2), g.traversal().V().has("colors", CP.eqIgnoreCase("white")).toSet());
        });

    }

    @Test
    public void t_EntityTest1() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("Name").build();
        g.getIndexManager().create().stringIndex().onVertexProperty("Kind").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        // g.tx().commitIncremental();
        Vertex entity1 = g.addVertex(T.id, "id1", "Name", "name1", "Kind", "entity", "Site", "propertyA", "OLA", "Gold");
        Vertex entity2 = g.addVertex(T.id, "id2", "Name", "name2", "Kind", "entity", "Site", "propertyB");
        // g.tx().commitIncremental();
        g.tx().commit();

        // assert that we find the entities
        assertEquals(entity1, Iterables.getOnlyElement(g.traversal().V().has("Name", "name1").has("Kind", "entity").toSet()));
        assertEquals(entity2, Iterables.getOnlyElement(g.traversal().V().has("Name", "name2").has("Kind", "entity").toSet()));
        g.tx().rollback();

        // perform some modifications on the vertices
        g.tx().open();
        // g.tx().commitIncremental();
        entity1.property("Name", "propertyA");
        entity1.property("Site").remove();
        entity1.property("OLA").remove();
        entity2.property("Name", "propertyB");
        entity2.property("Site").remove();
        entity2.property("OLA").remove();
        // g.tx().commitIncremental();
        g.tx().commit();

        // assert that we find the entities
        assertEquals(entity1, Iterables.getOnlyElement(g.traversal().V().has("Name", "propertyA").has("Kind", "entity").toSet()));
        assertEquals(entity2, Iterables.getOnlyElement(g.traversal().V().has("Name", "propertyB").has("Kind", "entity").toSet()));
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void deleteTest() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("firstName").build();
        g.getIndexManager().create().stringIndex().onVertexProperty("lastName").build();
        g.getIndexManager().create().stringIndex().onEdgeProperty("kind").build();
        g.getIndexManager().reindexAll();

        Vertex v1 = g.addVertex("firstName", "John", "lastName", "Doe");
        Vertex v2 = g.addVertex("firstName", "Jane", "lastName", "Doe");
        Vertex v3 = g.addVertex("firstName", "Jack", "lastName", "Johnson");
        Vertex v4 = g.addVertex("firstName", "Sarah", "lastName", "Doe");

        Set<Vertex> vertices = Sets.newHashSet(v1, v2, v3, v4);
        for (Vertex vA : vertices) {
            for (Vertex vB : vertices) {
                if (vA.equals(vB) == false) {
                    vA.addEdge("connect", vB, "kind", "connect");
                }
            }
        }

        assertEquals(3, (long)g.traversal().V().has("firstName", CP.startsWithIgnoreCase("j")).count().next());
        g.tx().commit();
        assertEquals(3, (long)g.traversal().V().has("firstName", CP.startsWithIgnoreCase("j")).count().next());

        int queryResultSize = 3;
        while (vertices.isEmpty() == false) {
            Vertex v = vertices.iterator().next();
            vertices.remove(v);
            if (((String) v.value("firstName")).startsWith("J")) {
                v.remove();
                queryResultSize--;
                assertEquals(queryResultSize, (long)g.traversal().V().has("firstName", CP.startsWithIgnoreCase("j")).count().next());
            } else {
                v.remove();
                assertEquals(queryResultSize, (long)g.traversal().V().has("firstName", CP.startsWithIgnoreCase("j")).count().next());
            }
        }
        assertEquals(0, queryResultSize);
        assertEquals(0, (long)g.traversal().V().has("firstName", CP.startsWithIgnoreCase("j")).count().next());
        assertEquals(0, (long)g.traversal().V().has("lastName", CP.matchesRegex(".*")).count().next());
        assertEquals(0, (long)g.traversal().E().has("kind", CP.matchesRegex(".*")).count().next());
    }

    @Test
    @Category(SlowTest.class)
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void massiveDeleteTest() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("firstName").build();
        g.getIndexManager().create().stringIndex().onVertexProperty("lastName").build();
        g.getIndexManager().reindexAll();

        Set<String> vertexIds = Sets.newHashSet();
        for (int i = 0; i < 10_000; i++) {
            Vertex v = g.addVertex("firstName", "John#" + i, "lastName", "Doe");
            vertexIds.add((String) v.id());
            if (i % 1_000 == 0) {
                g.tx().commitIncremental();
            }
        }
        assertEquals(10_000, (long)g.traversal().V().has("firstName", CP.startsWith("John")).count().next());
        g.tx().commit();
        assertEquals(10_000, (long)g.traversal().V().has("firstName", CP.startsWith("John")).count().next());

        for (int i = 0; i < 10_000; i++) {
            Vertex v = Iterators.getOnlyElement(g.vertices(vertexIds.iterator().next()), null);
            assertNotNull(v);
            vertexIds.remove(v.id());
            v.remove();
            if (i % 1_000 == 0) {
                assertEquals(vertexIds.size(), (long)g.traversal().V().has("firstName", CP.startsWith("John")).count().next());
                g.tx().commitIncremental();
                assertEquals(vertexIds.size(), (long)g.traversal().V().has("firstName", CP.startsWith("John")).count().next());
            }
        }
        assertEquals(0, (long)g.traversal().V().has("firstName", CP.startsWith("John")).count().next());
        g.tx().commit();
        assertEquals(0, (long)g.traversal().V().has("firstName", CP.startsWith("John")).count().next());

    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void trackingDownTheGhostVertexTest() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("kind").build();
        g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
        g.getIndexManager().reindexAll();

        g.addVertex("kind", "person", "name", "John");
        g.addVertex("kind", "person", "name", "Jane");
        g.tx().commit();

        g.getBranchManager().createBranch("test");

        // perform further inserts on master
        g.tx().open();
        Object ghostId = g.addVertex("kind", "person", "name", "Ghost").id();
        g.tx().commit();

        // perform an insert on "test"
        g.tx().open("test");
        g.addVertex("kind", "person", "name", "Jack");
        g.tx().commit();

        // delete the ghost on "master"
        g.tx().open();
        Iterators.getOnlyElement(g.vertices(ghostId)).remove();
        g.tx().commit();

        // assert that the ghost is not present on "master"
        g.tx().open();
        assertEquals(0, g.traversal().V().has("name", "Ghost").toSet().size());
        assertEquals(0, g.traversal().V().toStream().filter(v -> v.value("name").equals("Ghost")).count());
        assertEquals(0, g.traversal().V().has("kind", "person").toStream().filter(v -> v.value("name").equals("Ghost"))
            .count());
        g.tx().close();

        // assert that the ghost is not present on "test"
        g.tx().open("test");
        assertEquals(0, g.traversal().V().has("name", "Ghost").toSet().size());
        assertEquals(0, g.traversal().V().toStream().filter(v -> v.value("name").equals("Ghost")).count());
        assertEquals(0, g.traversal().V().has("kind", "person").toStream().filter(v -> v.value("name").equals("Ghost"))
            .count());
        g.tx().close();
    }

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "20")
    public void secondaryIndexingRemoveTestWithBranching() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("kind").build();
        g.getIndexManager().create().stringIndex().onVertexProperty("name").build();
        g.getIndexManager().reindexAll();

        Object johnsId = g.addVertex("kind", "person", "name", "John").id();
        Object janesId = g.addVertex("kind", "person", "name", "Jane").id();
        g.tx().commit();

        g.getBranchManager().createBranch("test");

        // perform a delete on "master"
        g.tx().open();
        Iterators.getOnlyElement(g.vertices(johnsId)).remove();
        g.tx().commit();

        // perform an insert on "test"
        g.tx().open("test");
        g.addVertex("kind", "person", "name", "Jack");
        g.tx().commit();

        // delete jane on "master"
        g.tx().open();
        Iterators.getOnlyElement(g.vertices(janesId)).remove();
        g.tx().commit();

        // assert that no persons are present on "master" anymore
        g.tx().open();
        assertEquals(0, g.traversal().V().has("kind", "person").toSet().size());
        assertEquals(0, g.traversal().V().toStream().filter(v -> v.value("kind").equals("person")).count());
        g.tx().close();

        // assert that the persons are still present on "test"
        g.tx().open("test");
        assertEquals(3, g.traversal().V().has("kind", "person").toSet().size());
        assertEquals(3, g.traversal().V().toStream().filter(v -> v.value("kind").equals("person")).count());
        g.tx().close();
    }
}
