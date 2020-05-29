package org.chronos.chronograph.test.cases.transaction;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronodb.api.ChronoDBFeatures;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ElementHistoryTest extends AllChronoGraphBackendsTest {

    @Test
    public void canGetVertexHistory(){
        ChronoGraph g = this.getGraph();
        Vertex a = g.addVertex(T.id, "A", "value", 1);
        Vertex b = g.addVertex(T.id, "B", "value", 1);
        long afterFirstCommit = g.tx().commitAndReturnTimestamp();

        a.property("value", 2);
        long afterSecondCommit = g.tx().commitAndReturnTimestamp();

        b.property("value", 2);
        long afterThirdCommit = g.tx().commitAndReturnTimestamp();

        a.property("value", 3);
        long afterFourthCommit = g.tx().commitAndReturnTimestamp();

        a.addEdge("myLabel", b); // adding an edge should modify both adjacent vertices
        long afterFifthCommit = g.tx().commitAndReturnTimestamp();

        List<Long> historyOfA = Lists.newArrayList(g.getVertexHistory(a));
        assertThat(historyOfA, contains(afterFifthCommit, afterFourthCommit, afterSecondCommit, afterFirstCommit));

        List<Long> historyOfB = Lists.newArrayList(g.getVertexHistory(b));
        assertThat(historyOfB, contains(afterFifthCommit, afterThirdCommit, afterFirstCommit));

    }

    @Test
    public void canGetEdgeHistory(){
        ChronoGraph g = this.getGraph();
        Vertex a = g.addVertex(T.id, "A", "value", 1);
        Vertex b = g.addVertex(T.id, "B", "value", 1);
        Vertex c = g.addVertex(T.id, "C", "value", 1);
        Edge e1 = a.addEdge("l", b, T.id, "e1", "value", 1);
        Edge e2 = a.addEdge("l", c, T.id, "e2", "value", 1);
        long afterFirstCommit = g.tx().commitAndReturnTimestamp();

        Edge e3 = b.addEdge("l", c, T.id, "e3");
        b.property("value", 2);
        long afterSecondCommit = g.tx().commitAndReturnTimestamp();

        e1.property("value", 2);
        long afterThirdCommit = g.tx().commitAndReturnTimestamp();

        b.remove();
        long afterFourthCommit = g.tx().commitAndReturnTimestamp();

        List<Long> historyOfE1 = Lists.newArrayList(g.getEdgeHistory(e1));
        assertThat(historyOfE1, contains(afterFourthCommit, afterThirdCommit, afterFirstCommit));

        List<Long> historyOfE2 = Lists.newArrayList(g.getEdgeHistory(e2));
        assertThat(historyOfE2, contains(afterFirstCommit));

        List<Long> historyOfE3 = Lists.newArrayList(g.getEdgeHistory(e3));
        assertThat(historyOfE3, contains(afterFourthCommit, afterSecondCommit));
    }

    @Test
    public void canGetVertexHistoryAcrossRollover(){
        ChronoGraph g = this.getGraph();
        ChronoDBFeatures features = ((ChronoGraphInternal) g).getBackingDB().getFeatures();
        Assume.assumeTrue(features.isPersistent());
        Assume.assumeTrue(features.isRolloverSupported());

        Vertex a = g.addVertex(T.id, "A", "value", 1);
        Vertex b = g.addVertex(T.id, "B", "value", 1);
        long afterFirstCommit = g.tx().commitAndReturnTimestamp();

        a.property("value", 2);
        long afterSecondCommit = g.tx().commitAndReturnTimestamp();

        b.property("value", 2);
        long afterThirdCommit = g.tx().commitAndReturnTimestamp();

        g.getMaintenanceManager().performRolloverOnMaster();

        a.property("value", 3);
        long afterFourthCommit = g.tx().commitAndReturnTimestamp();

        a.addEdge("myLabel", b); // adding an edge should modify both adjacent vertices
        long afterFifthCommit = g.tx().commitAndReturnTimestamp();

        g = this.closeAndReopenGraph();

        // history within chunk 0
        g.tx().open(afterThirdCommit);
        List<Long> historyOfAInChunk0 = Lists.newArrayList(g.getVertexHistory(a));
        assertThat(historyOfAInChunk0, contains(afterSecondCommit, afterFirstCommit));
        g.tx().rollback();

        // history within entire db
        g.tx().open();
        List<Long> historyOfA = Lists.newArrayList(g.getVertexHistory(a));
        assertThat(historyOfA, contains(afterFifthCommit, afterFourthCommit, afterSecondCommit, afterFirstCommit));
    }

    @Test
    public void canGetEdgeHistoryAcrossRollover(){
        ChronoGraph g = this.getGraph();
        ChronoDBFeatures features = ((ChronoGraphInternal) g).getBackingDB().getFeatures();
        Assume.assumeTrue(features.isPersistent());
        Assume.assumeTrue(features.isRolloverSupported());
        Vertex a = g.addVertex(T.id, "A", "value", 1);
        Vertex b = g.addVertex(T.id, "B", "value", 1);
        Vertex c = g.addVertex(T.id, "C", "value", 1);
        Edge e1 = a.addEdge("l", b, T.id, "e1", "value", 1);
        Edge e2 = a.addEdge("l", c, T.id, "e2", "value", 1);
        long afterFirstCommit = g.tx().commitAndReturnTimestamp();

        Edge e3 = b.addEdge("l", c, T.id, "e3");
        b.property("value", 2);
        long afterSecondCommit = g.tx().commitAndReturnTimestamp();

        g.getMaintenanceManager().performRolloverOnMaster();

        e1.property("value", 2);
        long afterThirdCommit = g.tx().commitAndReturnTimestamp();

        b.remove();
        long afterFourthCommit = g.tx().commitAndReturnTimestamp();

        g = this.closeAndReopenGraph();

        // history within chunk 0
        g.tx().open(afterSecondCommit);
        List<Long> historyOfE1InChunk0 = Lists.newArrayList(g.getEdgeHistory(e1));
        assertThat(historyOfE1InChunk0, contains(afterFirstCommit));
        List<Long> historyOfE2InChunk0 = Lists.newArrayList(g.getEdgeHistory(e2));
        assertThat(historyOfE2InChunk0, contains(afterFirstCommit));
        List<Long> historyOfE3InChunk0 = Lists.newArrayList(g.getEdgeHistory(e3));
        assertThat(historyOfE3InChunk0, contains(afterSecondCommit));
        g.tx().rollback();

        // history within entire db
        g.tx().open();
        List<Long> historyOfE1 = Lists.newArrayList(g.getEdgeHistory(e1));
        assertThat(historyOfE1, contains(afterFourthCommit, afterThirdCommit, afterFirstCommit));
        List<Long> historyOfE2 = Lists.newArrayList(g.getEdgeHistory(e2));
        assertThat(historyOfE2, contains(afterFirstCommit));
        List<Long> historyOfE3 = Lists.newArrayList(g.getEdgeHistory(e3));
        assertThat(historyOfE3, contains(afterFourthCommit, afterSecondCommit));
    }

}
