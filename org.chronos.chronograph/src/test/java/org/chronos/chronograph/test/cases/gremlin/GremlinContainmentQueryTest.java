package org.chronos.chronograph.test.cases.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class GremlinContainmentQueryTest extends AllChronoGraphBackendsTest {

    @Test
    public void canPerformWithinQueryWithInStringsClause() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "name", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "name", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "name", "Jack", "lastname", "Smith");
        g.addVertex(T.id, "5", "name", "John", "lastname", "Smith");

        this.assertCommitAssert(() -> {
            Set<Object> johnAndJaneIds = g.traversal().V()
                .has("name", P.within("John", "Jane"))
                .id()
                .toSet();
            assertThat(johnAndJaneIds, containsInAnyOrder("1", "2", "5"));
        });
    }

    @Test
    public void canPerformWithinQueryWithManyInStringsClause() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "name", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "name", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "name", "Jack", "lastname", "Smith");
        g.addVertex(T.id, "5", "name", "John", "lastname", "Smith");

        this.assertCommitAssert(() -> {
            Set<Object> johnAndJaneIds = g.traversal().V()
                .has("name", P.within("John", "Jane", "hello", "wold", "foo", "bar", "baz"))
                .id()
                .toSet();
            assertThat(johnAndJaneIds, containsInAnyOrder("1", "2", "5"));
        });
    }

    @Test
    public void canPerformWithinQueryWithInLongsClause() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().longIndex().onVertexProperty("age").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "Alice", "age", 15);
        g.addVertex(T.id, "2", "name", "Bob", "age", 23);
        g.addVertex(T.id, "3", "name", "Ceasar", "age", 58);
        g.addVertex(T.id, "4", "name", "Dorian", "age", 35);
        g.addVertex(T.id, "5", "name", "Emilia", "age", 58);

        this.assertCommitAssert(() -> {
            Set<Object> alCeEmIds = g.traversal().V()
                .has("age", P.within(15, 58))
                .id()
                .toSet();
            assertThat(alCeEmIds, containsInAnyOrder("1", "3", "5"));
        });
    }

    @Test
    public void canPerformWithinQueryWithManyInLongsClause() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().longIndex().onVertexProperty("age").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "Alice", "age", 15);
        g.addVertex(T.id, "2", "name", "Bob", "age", 23);
        g.addVertex(T.id, "3", "name", "Ceasar", "age", 58);
        g.addVertex(T.id, "4", "name", "Dorian", "age", 35);
        g.addVertex(T.id, "5", "name", "Emilia", "age", 58);

        this.assertCommitAssert(() -> {
            Set<Object> alCeEmIds = g.traversal().V()
                .has("age", P.within(15, 58, 122, 343432, 32423, 34354, 12231))
                .id()
                .toSet();
            assertThat(alCeEmIds, containsInAnyOrder("1", "3", "5"));
        });
    }

    @Test
    public void canPerformWithinQueryWithInDoublesClause() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("age").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "Alice", "age", 15.3);
        g.addVertex(T.id, "2", "name", "Bob", "age", 23.5);
        g.addVertex(T.id, "3", "name", "Ceasar", "age", 58.7);
        g.addVertex(T.id, "4", "name", "Dorian", "age", 35.9);
        g.addVertex(T.id, "5", "name", "Emilia", "age", 58.1);

        this.assertCommitAssert(() -> {
            Set<Object> aliceAndCeasarIds = g.traversal().V()
                .has("age", P.within(15.3, 58.7))
                .id()
                .toSet();
            assertThat(aliceAndCeasarIds, containsInAnyOrder("1", "3"));
        });

    }

    @Test
    public void canPerformWithinQueryWithManyInDoublesClause() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("age").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "Alice", "age", 15.3);
        g.addVertex(T.id, "2", "name", "Bob", "age", 23.5);
        g.addVertex(T.id, "3", "name", "Ceasar", "age", 58.7);
        g.addVertex(T.id, "4", "name", "Dorian", "age", 35.9);
        g.addVertex(T.id, "5", "name", "Emilia", "age", 58.1);

        this.assertCommitAssert(() -> {
            Set<Object> aliceAndCeasarIds = g.traversal().V()
                .has("age", P.within(15.3, 58.7, 43534.2343, 4545.2234, 56.33324, 5652.334))
                .id()
                .toSet();
            assertThat(aliceAndCeasarIds, containsInAnyOrder("1", "3"));
        });

    }

    @Test
    public void canPerformWithoutQueryWithInStringsClause(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "name", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "name", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "name", "Jack", "lastname", "Smith");
        g.addVertex(T.id, "5", "name", "John", "lastname", "Smith");

        this.assertCommitAssert(()->{
            Set<Object> notJohnAndJaneIds = g.traversal().V()
                .has("name", P.without("John", "Jane"))
                .id()
                .toSet();
            assertThat(notJohnAndJaneIds, containsInAnyOrder("3", "4"));
        });
    }

    @Test
    public void canPerformWithoutQueryWithManyInStringsClause(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "name", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "name", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "name", "Jack", "lastname", "Smith");
        g.addVertex(T.id, "5", "name", "John", "lastname", "Smith");

        this.assertCommitAssert(()->{
            Set<Object> notJohnAndJaneIds = g.traversal().V()
                .has("name", P.without("John", "Jane", "foo", "bar", "baz", "hello"))
                .id()
                .toSet();
            assertThat(notJohnAndJaneIds, containsInAnyOrder("3", "4"));
        });
    }

    @Test
    public void canPerformWithoutQueryWithInLongsClause(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().longIndex().onVertexProperty("age").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "Alice", "age", 15);
        g.addVertex(T.id, "2", "name", "Bob", "age", 23);
        g.addVertex(T.id, "3", "name", "Ceasar", "age", 58);
        g.addVertex(T.id, "4", "name", "Dorian", "age", 35);
        g.addVertex(T.id, "5", "name", "Emilia", "age", 58);

        this.assertCommitAssert(()->{
            Set<Object> bobAndDorianIds = g.traversal().V()
                .has("age", P.without(15, 58))
                .id()
                .toSet();
            assertThat(bobAndDorianIds, containsInAnyOrder("2", "4"));
        });
    }

    @Test
    public void canPerformWithoutQueryWithManyInLongsClause(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().longIndex().onVertexProperty("age").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "Alice", "age", 15);
        g.addVertex(T.id, "2", "name", "Bob", "age", 23);
        g.addVertex(T.id, "3", "name", "Ceasar", "age", 58);
        g.addVertex(T.id, "4", "name", "Dorian", "age", 35);
        g.addVertex(T.id, "5", "name", "Emilia", "age", 58);

        this.assertCommitAssert(()->{
            Set<Object> bobAndDorianIds = g.traversal().V()
                .has("age", P.without(15, 58, 123, 456, 789, 222))
                .id()
                .toSet();
            assertThat(bobAndDorianIds, containsInAnyOrder("2", "4"));
        });
    }

    @Test
    public void canPerformWithoutQueryWithInDoublesClause(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("age").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "Alice", "age", 15.3);
        g.addVertex(T.id, "2", "name", "Bob", "age", 23.5);
        g.addVertex(T.id, "3", "name", "Ceasar", "age", 58.7);
        g.addVertex(T.id, "4", "name", "Dorian", "age", 35.9);
        g.addVertex(T.id, "5", "name", "Emilia", "age", 58.1);

        this.assertCommitAssert(()->{
            Set<Object> notAliceAndCeasarIds = g.traversal().V()
                .has("age", P.without(15.3, 58.7))
                .id()
                .toSet();
            assertThat(notAliceAndCeasarIds, containsInAnyOrder("2", "4", "5"));
        });
    }

    @Test
    public void canPerformWithoutQueryWithManyInDoublesClause(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("age").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "name", "Alice", "age", 15.3);
        g.addVertex(T.id, "2", "name", "Bob", "age", 23.5);
        g.addVertex(T.id, "3", "name", "Ceasar", "age", 58.7);
        g.addVertex(T.id, "4", "name", "Dorian", "age", 35.9);
        g.addVertex(T.id, "5", "name", "Emilia", "age", 58.1);

        this.assertCommitAssert(()->{
            Set<Object> notAliceAndCeasarIds = g.traversal().V()
                .has("age", P.without(15.3, 58.7, 3.1415, 423.3423, 34.3456, 66.743))
                .id()
                .toSet();
            assertThat(notAliceAndCeasarIds, containsInAnyOrder("2", "4", "5"));
        });
    }
}
