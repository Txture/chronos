package org.chronos.chronograph.test.cases.transaction;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.history.RestoreResult;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class HistoryManagerTest extends AllChronoGraphBackendsTest {

    @Test
    public void restoringNonExistentVertexHasNoEffect() {
        ChronoGraph g = this.getGraph();
        g.tx().open();
        RestoreResult result = g.getHistoryManager().restoreVertexAsOf(0, "bullshit-id");
        assertThat(result.getFailedEdgeIds().isEmpty(), is(true));
        assertThat(result.getSuccessfullyRestoredEdgeIds().isEmpty(), is(true));
        assertThat(result.getSuccessfullyRestoredVertexIds(), contains("bullshit-id"));

        ChronoGraphTransaction tx = g.tx().getCurrentTransaction();
        assertThat(tx.getContext().isDirty(), is(false));
    }

    @Test
    public void canRestoreVertexFromNonExistentState() {
        ChronoGraph g = this.getGraph();
        g.tx().open();

        Vertex vertex = g.addVertex("firstName", "John", "lastName", "Doe");
        String vertexId = (String) vertex.id();
        g.tx().commit();

        g.tx().open();
        g.getHistoryManager().restoreVertexAsOf(0L, vertexId);

        ChronoGraphTransaction tx = g.tx().getCurrentTransaction();
        assertThat(tx.getContext().isDirty(), is(true));
        assertThat(tx.getContext().getModifiedVertices().size(), is(1));
        assertThat(Iterables.getOnlyElement(tx.getContext().getModifiedVertices()).id(), is(vertexId));

        // the vertex should not exist anymore, as it has been restored from timestamp 0 (where it did not exist)
        assertThat(g.vertices(vertexId).hasNext(), is(false));
    }

    @Test
    public void canRestoreVertexFromExistingState() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        Vertex vertex = g.addVertex("firstName", "John", "lastName", "Doe");
        String vertexId = (String) vertex.id();
        g.tx().commit();

        long afterFirstCommit = g.getNow();

        g.tx().open();
        g.vertices(vertexId).next().property("lastName", "Smith");
        g.tx().commit();

        g.tx().open();
        g.getHistoryManager().restoreVertexAsOf(afterFirstCommit, vertexId);

        ChronoGraphTransaction tx = g.tx().getCurrentTransaction();
        assertThat(tx.getContext().isDirty(), is(true));
        assertThat(tx.getContext().getModifiedVertices().size(), is(1));
        assertThat(Iterables.getOnlyElement(tx.getContext().getModifiedVertices()).id(), is(vertexId));

        Vertex restoredVertex = Iterators.getOnlyElement(g.vertices(vertexId));
        assertThat(restoredVertex.value("firstName"), is("John"));
        assertThat(restoredVertex.value("lastName"), is("Doe"));
    }

    @Test
    public void restoringAVertexDropsItsCurrentState() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        Vertex vertex = g.addVertex("firstName", "John", "lastName", "Doe");
        String vertexId = (String) vertex.id();
        g.tx().commit();

        long afterFirstCommit = g.getNow();

        g.tx().open();
        Vertex v = g.vertices(vertexId).next();
        v.property("firstName").remove();
        v.property("age", 30);
        v.addEdge("self", v);
        g.getHistoryManager().restoreVertexAsOf(afterFirstCommit, vertexId);

        assertThat(v.value("firstName"), is("John"));
        assertThat(v.value("lastName"), is("Doe"));
        assertThat(v.edges(Direction.BOTH).hasNext(), is(false));
        assertThat(g.edges().hasNext(), is(false));
    }

    @Test
    public void restoringAVertexAlsoRestoresTheEdges() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        ChronoVertex vJohn = (ChronoVertex) g.addVertex("firstName", "John", "lastName", "Doe");
        ChronoVertex vJane = (ChronoVertex) g.addVertex("firstName", "Jane", "lastName", "Doe");
        ChronoVertex vJack = (ChronoVertex) g.addVertex("firstName", "Jack", "lastName", "Smith");
        vJohn.addEdge("marriedTo", vJane);
        vJack.addEdge("friend", vJohn);
        g.tx().commit();

        long afterFirstCommit = g.getNow();

        g.tx().open();
        vJohn.remove();
        g.tx().commit();

        g.tx().open();
        assertThat(g.vertices(vJohn).hasNext(), is(false));
        RestoreResult restoreResult = g.getHistoryManager().restoreVertexAsOf(afterFirstCommit, vJohn.id());
        assertThat(restoreResult.getSuccessfullyRestoredVertexIds(), contains(vJohn.id()));
        assertThat(restoreResult.getSuccessfullyRestoredEdgeIds().size(), is(2));
        assertThat(restoreResult.getFailedEdgeIds().size(), is(0));
        assertThat(vJohn.isRemoved(), is(false));
        assertThat(vJack.vertices(Direction.OUT, "friend").next(), is(vJohn));
        assertThat(vJane.vertices(Direction.IN, "marriedTo").next(), is(vJohn));
        assertThat(vJohn.vertices(Direction.OUT, "marriedTo").next(), is(vJane));
        assertThat(vJohn.vertices(Direction.IN, "friend").next(), is(vJack));
    }

    @Test
    public void nonRestorableEdgesAreReported() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        ChronoVertex vJohn = (ChronoVertex) g.addVertex("firstName", "John", "lastName", "Doe");
        ChronoVertex vJane = (ChronoVertex) g.addVertex("firstName", "Jane", "lastName", "Doe");
        ChronoVertex vJack = (ChronoVertex) g.addVertex("firstName", "Jack", "lastName", "Smith");
        vJohn.addEdge("marriedTo", vJane);
        vJack.addEdge("friend", vJohn);
        g.tx().commit();

        long afterFirstCommit = g.getNow();

        g.tx().open();
        vJohn.remove();
        vJane.remove();
        g.tx().commit();

        g.tx().open();
        assertThat(g.vertices(vJohn).hasNext(), is(false));
        RestoreResult restoreResult = g.getHistoryManager().restoreVertexAsOf(afterFirstCommit, vJohn.id());
        assertThat(restoreResult.getSuccessfullyRestoredVertexIds(), contains(vJohn.id()));
        assertThat(restoreResult.getSuccessfullyRestoredEdgeIds().size(), is(1));
        assertThat(restoreResult.getFailedEdgeIds().size(), is(1));
        assertThat(vJohn.isRemoved(), is(false));
        assertThat(vJack.vertices(Direction.OUT, "friend").next(), is(vJohn));
        assertThat(vJohn.vertices(Direction.OUT, "marriedTo").hasNext(), is(false));
        assertThat(vJohn.vertices(Direction.IN, "friend").next(), is(vJack));
    }

    @Test
    public void canRestoreGraphStateAsOfTimestamp() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        ChronoVertex vJohn = (ChronoVertex) g.addVertex("firstName", "John", "lastName", "Doe");
        ChronoVertex vJane = (ChronoVertex) g.addVertex("firstName", "Jane", "lastName", "Doe");
        ChronoVertex vJack = (ChronoVertex) g.addVertex("firstName", "Jack", "lastName", "Smith");
        vJohn.addEdge("marriedTo", vJane);
        vJack.addEdge("friend", vJohn);
        g.tx().commit();

        long afterFirstCommit = g.getNow();

        g.tx().open();
        vJohn.property("lastName", "Smith");
        vJack.remove();
        ChronoVertex vSarah = (ChronoVertex) g.addVertex("firstName", "Sarah", "lastName", "Johnson");
        vJane.addEdge("friend", vSarah);
        g.tx().commit();

        g.tx().open();
        RestoreResult restoreResult = g.getHistoryManager().restoreGraphStateAsOf(0);
        assertThat(restoreResult.getSuccessfullyRestoredVertexIds().size(), is(4));
        assertThat(restoreResult.getSuccessfullyRestoredEdgeIds().size(), is(3));
        assertThat(restoreResult.getFailedEdgeIds().size(), is(0));
        // the graph should now be empty
        assertThat(Iterators.size(g.vertices()), is(0));
        assertThat(Iterators.size(g.edges()), is(0));
        g.tx().rollback();

        g.tx().open();
        RestoreResult restoreResult2 = g.getHistoryManager().restoreGraphStateAsOf(afterFirstCommit);
        assertThat(restoreResult2.getSuccessfullyRestoredVertexIds().size(), is(4));
        assertThat(restoreResult2.getSuccessfullyRestoredEdgeIds().size(), is(3));
        assertThat(restoreResult2.getFailedEdgeIds().size(), is(0));
        assertThat(g.vertices(vSarah.id()).hasNext(), is(false));
        assertThat(vJane.edges(Direction.OUT,"friend").hasNext(), is(false));
        assertThat(vJohn.value("lastName"), is("Doe"));
        assertThat(vJack.isRemoved(), is(false));
        g.tx().commit();

        g.tx().open();
        assertThat(g.vertices(vSarah.id()).hasNext(), is(false));
        assertThat(vJane.edges(Direction.OUT,"friend").hasNext(), is(false));
        assertThat(vJohn.value("lastName"), is("Doe"));
        assertThat(vJack.isRemoved(), is(false));
    }
}
