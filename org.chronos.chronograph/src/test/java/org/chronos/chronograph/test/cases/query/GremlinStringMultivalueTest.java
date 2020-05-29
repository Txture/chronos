package org.chronos.chronograph.test.cases.query;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.structure.T;
import org.chronos.chronograph.api.builder.query.CP;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.chronograph.test.base.FailOnAllEdgesQuery;
import org.chronos.chronograph.test.base.FailOnAllVerticesQuery;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class GremlinStringMultivalueTest extends AllChronoGraphBackendsTest {

    @Test
    public void canAnswerEqualsQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", "hello").id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerEqualsQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", "hello").id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    public void canAnswerNotEqualsQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.neq("hello")).id().toSet();
            assertThat(ids, containsInAnyOrder("5", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotEqualsQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.neq("hello")).id().toSet();
            assertThat(ids, containsInAnyOrder("5", "6"));
        });
    }

    @Test
    public void canAnswerWithinQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.within("hello", "bar")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerWithinQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.within("hello", "bar")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerWithoutQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.without("hello", "bar")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerWithoutQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", P.without("hello", "bar")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerStartsWithQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "height"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.startingWith("he")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerStartsWithQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "height"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.startingWith("he")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerNotStartsWithQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "height"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.notStartingWith("he")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotStartsWithQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "height"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.notStartingWith("he")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerEndsWithQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yolo"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.endingWith("lo")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerEndsWithQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yolo"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.endingWith("lo")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerNotEndsWithQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yolo"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.notEndingWith("lo")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotEndsWithQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yolo"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.notEndingWith("lo")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerContainsQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.containing("el")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerContainsQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.containing("el")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerNotContainsQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.notContaining("el")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotContainsQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", TextP.notContaining("el")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }


    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerMatchesRegexQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.matchesRegex(".*el.*")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }


    @Test
    public void canAnswerMatchesRegexQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.matchesRegex(".*el.*")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotMatchesRegexQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notMatchesRegex(".*el.*")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerNotMatchesRegexQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notMatchesRegex(".*el.*")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }


    // =================================================================================================================
    // IGNORE CASE TESTS
    // =================================================================================================================

    @Test
    public void canAnswerEqualsIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.eqIgnoreCase("HELLO")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerEqualsIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.eqIgnoreCase("HELLO")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4"));
        });
    }

    @Test
    public void canAnswerNotEqualsIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.neqIgnoreCase("HELLO")).id().toSet();
            assertThat(ids, containsInAnyOrder("5", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotEqualsIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.neqIgnoreCase("HELLO")).id().toSet();
            assertThat(ids, containsInAnyOrder("5", "6"));
        });
    }

    @Test
    public void canAnswerWithinIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.withinIgnoreCase(Lists.newArrayList("HELLO", "BAR"))).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerWithinIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.withinIgnoreCase(Lists.newArrayList("HELLO", "BAR"))).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerWithoutIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.withoutIgnoreCase(Lists.newArrayList("HELLO", "BAR"))).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerWithoutIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "bar"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.withoutIgnoreCase(Lists.newArrayList("HELLO", "BAR"))).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerStartsWithIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "height"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.startsWithIgnoreCase("HE")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerStartsWithIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "height"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.startsWithIgnoreCase("HE")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerNotStartsWithIgnoreQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "height"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notStartsWithIgnoreCase("HE")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotStartsWithIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "height"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notStartsWithIgnoreCase("HE")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerEndsWithIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yolo"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.endsWithIgnoreCase("LO")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerEndsWithIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yolo"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.endsWithIgnoreCase("LO")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerNotEndsWithIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yolo"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notEndsWithIgnoreCase("LO")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotEndsWithIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yolo"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notEndsWithIgnoreCase("LO")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerContainsIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.containsIgnoreCase("EL")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerContainsIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.containsIgnoreCase("EL")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    public void canAnswerNotContainsIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notContainsIgnoreCase("EL")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }


    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerMatchesRegexIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.matchesRegexIgnoreCase(".*EL.*")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }


    @Test
    public void canAnswerMatchesRegexIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.matchesRegexIgnoreCase(".*EL.*")).id().toSet();
            assertThat(ids, containsInAnyOrder("2", "3", "4", "6"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canAnswerNotMatchesRegexIgnoreCaseQueryOnIndexedStringMultivalues() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notMatchesRegexIgnoreCase(".*EL.*")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    @Test
    public void canAnswerNotMatchesRegexIgnoreCaseQueryOnUnindexedStringMultivalues() {
        ChronoGraph g = this.getGraph();

        g.tx().open();
        g.addVertex(T.id, "1");
        g.addVertex(T.id, "2", "p", "hello");
        g.addVertex(T.id, "3", "p", Lists.newArrayList("hello"));
        g.addVertex(T.id, "4", "p", Lists.newArrayList("hello", "world"));
        g.addVertex(T.id, "5", "p", "foo");
        g.addVertex(T.id, "6", "p", Lists.newArrayList("foo", "yellow"));

        this.assertCommitAssert(() -> {
            Set<Object> ids = g.traversal().V().has("p", CP.notMatchesRegexIgnoreCase(".*EL.*")).id().toSet();
            assertThat(ids, containsInAnyOrder("5"));
        });
    }

    // =================================================================================================================
    // NEGATIVE TESTS
    // =================================================================================================================

    @Test
    public void cannotUseMultipleStringValuesForEqualityChecking() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        try {
            g.traversal().V().has("p", P.eq(Lists.newArrayList("hello"))).id().toSet();
            fail("managed to create gremlin query where has(...) EQUALS predicate has multiple values!");
        } catch (IllegalArgumentException expected) {
            // pass
        }
    }

    @Test
    public void cannotUseMultipleStringValuesForInequalityChecking() {
        ChronoGraph g = this.getGraph();
        g.getIndexManager().create().stringIndex().onVertexProperty("p").build();
        g.getIndexManager().reindexAll();

        g.tx().open();
        try {
            g.traversal().V().has("p", P.neq(Lists.newArrayList("hello"))).id().toSet();
            fail("managed to create gremlin query where has(...) NOT EQUALS predicate has multiple values!");
        } catch (IllegalArgumentException expected) {
            // pass
        }
    }

}
