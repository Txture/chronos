package org.chronos.chronograph.test.cases.query;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.chronograph.test.base.FailOnAllEdgesQuery;
import org.chronos.chronograph.test.base.FailOnAllVerticesQuery;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class GremlinDoubleMultivalueTest extends AllChronoGraphBackendsTest {

    @Test
    public void canAnswerEqualsQueryOnUnindexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", 100.0).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerEqualsQueryOnIndexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", 100.0).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    public void canAnswerNotEqualsQueryOnUnindexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.neq(100.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("5", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotEqualsQueryOnIndexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.neq(100.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("5", "6"));
        });
    }


    @Test
    public void canAnswerWithinQueryOnUnindexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.within(100.0, 600.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerWithinQueryOnIndexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.within(100.0, 600.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerWithoutQueryOnUnindexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.without(100.0, 600.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerWithoutQueryOnIndexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.without(100.0, 600.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerLessThanQueryOnUnindexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.lt(500.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerLessThanQueryOnIndexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.lt(500.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    public void canAnswerLessThanOrEqualToQueryOnUnindexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));
        g.addVertex(T.id, "7", "p", Lists.newArrayList(1000.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.lte(500.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "5", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerLessOrEqualToThanQueryOnIndexedDoubleMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100.0);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100.0));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100.0, 200.0));
        g.addVertex(T.id, "5", "p", 500.0);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500.0, 600.0));
        g.addVertex(T.id, "7", "p", Lists.newArrayList(1000.0));

        this.assertCommitAssert(()->{
            Set<Object> ids = g.traversal().V().has("p", P.lte(500.0)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "5", "6"));
        });
    }

    // =================================================================================================================
    // NEGATIVE TESTS
    // =================================================================================================================

    @Test
    public void cannotUseMultipleDoubleValuesForEqualityChecking() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("p").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.tx().open();
        try {
            g.traversal().V().has("p", Lists.newArrayList(100.0)).id().toSet();
            fail("managed to create gremlin query where has(...) EQUALS predicate has multiple values!");
        } catch (IllegalArgumentException expected) {
            // pass
        }
    }

}
