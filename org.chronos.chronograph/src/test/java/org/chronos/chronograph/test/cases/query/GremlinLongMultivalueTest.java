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

public class GremlinLongMultivalueTest extends AllChronoGraphBackendsTest {

    @Test
    public void canAnswerEqualsQueryOnUnindexedLongMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", 100L).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerEqualsQueryOnIndexedLongMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", 100L).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });

    }

    @Test
    public void canAnswerNotEqualsQueryOnUnindexedLongMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500, 600));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.neq(100L)).id().toSet();
            assertThat(ids, containsInAnyOrder("5", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotEqualsQueryOnIndexedLongMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500, 600));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.neq(100L)).id().toSet();
            assertThat(ids, containsInAnyOrder("5", "6"));
        });

    }

    @Test
    public void canAnswerWithinQueryOnUnindexedLongMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.within(100L, 600L)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerWithinQueryOnIndexedLongMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.within(100L, 600L)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });

    }

    @Test
    public void canAnswerWithoutQueryOnUnindexedLongMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.without(100L, 600L)).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerWithoutQueryOnIndexedLongMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.without(100L, 600L)).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerLessThanQueryOnUnindexedLongMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.lt(200)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerLessThanQueryOnIndexedLongMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.lt(200)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    public void canAnswerLessThanOrEqualToQueryOnUnindexedLongMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.lte(100)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerLessThanOrEqualToQueryOnIndexedLongMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.lte(100)).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }





    @Test
    public void canAnswerGreaterThanQueryOnUnindexedLongMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.gt(100)).id().toSet();
            assertThat(ids, containsInAnyOrder("4", "5", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerGreaterThanQueryOnIndexedLongMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.gt(100)).id().toSet();
            assertThat(ids, containsInAnyOrder("4", "5", "6"));
        });
    }

    @Test
    public void canAnswerGreaterThanOrEqualToQueryOnUnindexedLongMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.gte(200)).id().toSet();
            assertThat(ids, containsInAnyOrder("4", "5", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerGreaterThanOrEqualToQueryOnIndexedLongMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", 100L);
        g.addVertex(T.id, "3", "p", Lists.newArrayList(100L));
        g.addVertex(T.id, "4", "p", Lists.newArrayList(100L, 200L));
        g.addVertex(T.id, "5", "p", 500);
        g.addVertex(T.id, "6", "p", Lists.newArrayList(500L, 600L));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.gte(200)).id().toSet();
            assertThat(ids, containsInAnyOrder("4", "5", "6"));
        });
    }


    // =================================================================================================================
    // NEGATIVE TESTS
    // =================================================================================================================

    @Test
    public void cannotUseMultipleLongValuesForEqualityChecking() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().longIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        try {
            g.traversal().V().has("p", Lists.newArrayList(100L)).id().toSet();
            fail("managed to create gremlin query where has(...) EQUALS predicate has multiple values!");
        } catch (IllegalArgumentException expected) {
            // pass
            expected.printStackTrace();
        }
    }

}
