package org.chronos.chronograph.test.cases.iterators;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronograph.api.iterators.ChronoGraphIterators;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@Category(IntegrationTest.class)
public class GraphIteratorsTest extends AllChronoGraphBackendsTest {

    @Test
    public void canIterateOverAllVerticesAtHead() {
        ChronoGraph g = this.getGraph();
        g.tx().open();
        g.addVertex("firstName", "John", "lastName", "Doe");
        g.addVertex("firstName", "Jane", "lastName", "Doe");
        g.tx().commit();

        g.tx().open();
        g.addVertex("firstName", "Sarah", "lastName", "Doe");
        g.addVertex("firstName", "Jack", "lastName", "Smith");
        g.traversal().V().has("firstName", "John").forEachRemaining(Vertex::remove);
        g.tx().commit();

        Set<String> reportedFirstNames = Sets.newHashSet();
        ChronoGraphIterators.createIteratorOn(g)
            .overAllBranches()
            .atHead()
            .overAllVertices(state ->
                reportedFirstNames.add(state.getCurrentVertex().value("firstName"))
            );
        assertThat(reportedFirstNames, containsInAnyOrder("Jane", "Sarah", "Jack"));
    }

    @Test
    public void canIterateOverAllVerticesOnAllTimestamps() {
        ChronoGraph g = this.getGraph();
        g.tx().open();
        g.addVertex("firstName", "John", "lastName", "Doe");
        g.addVertex("firstName", "Jane", "lastName", "Doe");
        g.tx().commit();

        g.tx().open();
        g.addVertex("firstName", "Sarah", "lastName", "Doe");
        g.addVertex("firstName", "Jack", "lastName", "Smith");
        g.traversal().V().has("firstName", "John").forEachRemaining(Vertex::remove);
        g.tx().commit();

        Set<String> reportedFirstNames = Sets.newHashSet();
        ChronoGraphIterators.createIteratorOn(g)
            .overAllBranches()
            .overAllCommitTimestampsDescending()
            .overAllVertices(state ->
                reportedFirstNames.add(state.getCurrentVertex().value("firstName"))
            );
        assertThat(reportedFirstNames, containsInAnyOrder("John", "Jane", "Sarah", "Jack"));
    }

    @Test
    public void canIterateOverAllBranches() {
        ChronoGraph g = this.getGraph();
        g.tx().open();
        g.addVertex("firstName", "John", "lastName", "Doe");
        g.addVertex("firstName", "Jane", "lastName", "Doe");
        g.tx().commit();

        g.getBranchManager().createBranch("test");

        g.tx().open("test");
        g.addVertex("firstName", "Sarah", "lastName", "Doe");
        g.addVertex("firstName", "Jack", "lastName", "Smith");
        g.traversal().V().has("firstName", "John").forEachRemaining(Vertex::remove);
        g.tx().commit();

        Set<String> reportedFirstNames = Sets.newHashSet();
        Set<String> checkedBranchNames = Sets.newHashSet();

        ChronoGraphIterators.createIteratorOn(g)
            .overAllBranches((oldBranch, newBranch) -> checkedBranchNames.add(newBranch))
            .overAllCommitTimestampsDescending()
            .overAllVertices(state ->
                reportedFirstNames.add(state.getCurrentVertex().value("firstName"))
            );

        assertThat(reportedFirstNames, containsInAnyOrder("John", "Jane", "Sarah", "Jack"));
        assertThat(checkedBranchNames, containsInAnyOrder("test", ChronoDBConstants.MASTER_BRANCH_IDENTIFIER));
    }

    @Test
    public void canVisitChangeTimestampsOnAllBranches() {
        ChronoGraph g = this.getGraph();
        g.tx().open();
        g.addVertex("firstName", "John", "lastName", "Doe");
        g.addVertex("firstName", "Jane", "lastName", "Doe");
        long afterFirstCommit = g.tx().commitAndReturnTimestamp();

        g.getBranchManager().createBranch("test");

        g.tx().open("test");
        g.addVertex("firstName", "Sarah", "lastName", "Doe");
        g.addVertex("firstName", "Jack", "lastName", "Smith");
        g.traversal().V().has("firstName", "John").forEachRemaining(Vertex::remove);
        long afterSecondCommit = g.tx().commitAndReturnTimestamp();

        Set<Pair<String, Long>> visitedCoordinates = Sets.newHashSet();

        ChronoGraphIterators.createIteratorOn(g)
            .overAllBranches()
            .overAllCommitTimestampsDescending()
            .visitCoordinates(state ->
                visitedCoordinates.add(Pair.of(state.getBranch(), state.getTimestamp()))
            );

        assertThat(visitedCoordinates, containsInAnyOrder(
            Pair.of(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, afterFirstCommit),
            Pair.of("test", afterSecondCommit))
        );
    }

    @Test
    public void canIterateOverHistoryOfVertexOnAllBranches() {
        ChronoGraph g = this.getGraph();

        String id = "vJohn";
        g.tx().open();
        g.addVertex(T.id, id, "firstName", "John", "lastName", "Doe");
        long afterFirstCommit = g.tx().commitAndReturnTimestamp();

        g.getBranchManager().createBranch("test");

        g.tx().open("test");
        Iterators.getOnlyElement(g.vertices(id)).property("lastName", "Smith");
        long afterSecondCommit = g.tx().commitAndReturnTimestamp();

        Set<Pair<String, Long>> visitedCoordinates = Sets.newHashSet();

        ChronoGraphIterators.createIteratorOn(g)
            .overAllBranches()
            .overHistoryOfVertex(id)
            .visitCoordinates(state -> {
                visitedCoordinates.add(Pair.of(state.getBranch(), state.getTimestamp()));
            });


        assertThat(visitedCoordinates, containsInAnyOrder(
            Pair.of(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, afterFirstCommit),
            Pair.of("test", afterSecondCommit),
            Pair.of("test", afterFirstCommit))
        );
    }

}

