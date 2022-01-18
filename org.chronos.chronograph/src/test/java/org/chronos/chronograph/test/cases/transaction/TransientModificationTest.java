package org.chronos.chronograph.test.cases.transaction;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.structure.PropertyStatus;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class TransientModificationTest extends AllChronoGraphBackendsTest {

    @Test
    public void transientPropertiesOnVerticesAreReflectedInGremlinQueries() {
        ChronoGraph g = this.getGraph();
        g.addVertex("name", "martin");
        g.tx().commit();

        Vertex me = g.traversal().V().has("name", "martin").toSet().iterator().next();
        assertNotNull(me);

        // apply the transient change
        me.property("name", "john");

        { // test 1: try to retrieve the vertex by the original data state and assert that it's not present
            Set<Vertex> vertices = g.traversal().V().has("name", "martin").toSet();
            assertTrue("Found vertex 'martin' even though transient context renamed it to 'john'!", vertices.isEmpty());
        }

        { // test 2: try to retrieve the vertex by the transient data state and assert that it works
            Set<Vertex> vertices = g.traversal().V().has("name", "john").toSet();
            assertEquals("Did not find vertex 'john' even though transient context should contain it!", 1,
                vertices.size());
        }

        g.tx().commit();

        { // test 3: try to retrieve the vertex by the new persisted data state and assert that it works
            Set<Vertex> vertices = g.traversal().V().has("name", "john").toSet();
            assertEquals("Did not find vertex 'john' after commiting the change!", 1, vertices.size());
        }
    }

    @Test
    public void transientVertexDeletionsAreReflectedInGremlinQueries() {
        ChronoGraph g = this.getGraph();
        g.addVertex("name", "martin");
        g.addVertex("name", "john");
        g.tx().commit();

        Vertex me = g.traversal().V().has("name", "martin").toSet().iterator().next();
        assertNotNull(me);
        me.remove();

        // assert that the query no longer returns "me", even before committing the operation
        assertTrue("Transient vertex deletions are ignored by gremlin query!",
            g.traversal().V().has("name", "martin").toSet().isEmpty());
    }

    @Test
    public void transientEdgeDeletionsAreReflectedInGremlinQueries() {
        ChronoGraph g = this.getGraph();
        Vertex me = g.addVertex("name", "martin");
        Vertex john = g.addVertex("name", "john");
        me.addEdge("friend", john, "since", "forever");
        g.tx().commit();

        Edge e = g.traversal().E().has("since", "forever").toSet().iterator().next();
        assertNotNull(e);

        e.remove();

        // assert that the query no longer returns the edge, even before committing the operation
        assertTrue("Transient edge deletions are ignored by gremlin query!",
            g.traversal().E().has("since", "forever").toSet().isEmpty());
    }

    @Test
    public void transientAddedVerticesAreReflectedInGremlinQueries() {
        ChronoGraph g = this.getGraph();
        g.addVertex("name", "martin");
        g.addVertex("name", "john");
        // note: no commit here! Vertices are transient

        Iterator<Vertex> verticesIterator = g.vertices();
        assertEquals(2, Iterators.size(verticesIterator));
    }

    @Test
    public void transientGetVertexByIdWorks() {
        ChronoGraph g = this.getGraph();
        final Vertex v1 = g.addVertex();
        final Edge e1 = v1.addEdge("l", v1);
        g.tx().commit();
        assertEquals(v1.id(), g.vertices(v1.id()).next().id());
        assertEquals(e1.id(), g.edges(e1.id()).next().id());

        v1.property(VertexProperty.Cardinality.single, "name", "marko");
        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", g.vertices(v1.id()).next().<String>value("name"));
        g.tx().commit();
    }

    @Test
    public void canHandleDeletedQueryResultCandidatesInTransientState(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("firstname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "firstname", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "firstname", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "firstname", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "firstname", "John", "lastname", "Smith");

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("firstname", "John").id().toSet();
            assertThat(ids, containsInAnyOrder("1", "4"));
        });

        // now, delete John Smith...
        g.vertex("4").remove();
        // ... and add John Norman
        g.addVertex(T.id, "5", "firstname", "John", "lastname", "Norman");


        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("firstname", "John").id().toSet();
            assertThat(ids, containsInAnyOrder("1", "5"));
        });
    }

    @Test
    public void changingAPropertyBackToItsOriginalValueCausesCommitToBecomeEmpty(){
        ChronoGraph g = this.getGraph();
        ChronoVertex vJohn = (ChronoVertex) g.addVertex(T.id, "1", "firstName", "John", "lastName", "Doe");
        g.tx().commit();

        vJohn.property("lastName", "Smith");
        // this property is now modified...
        assertEquals(PropertyStatus.MODIFIED, vJohn.getPropertyStatus("lastName"));
        // ... and now we change it back
        vJohn.property("lastName", "Doe");
        // the property is still modified according to the transaction...
        assertEquals(PropertyStatus.MODIFIED, vJohn.getPropertyStatus("lastName"));

        // ... but if we commit now, we don't get a new timestamp,
        // because effectively we changed nothing.
        long timestamp = g.tx().commitAndReturnTimestamp();
        assertThat(timestamp, is(lessThan(0L)));
    }
}
