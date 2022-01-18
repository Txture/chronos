package org.chronos.chronograph.test.cases.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.chronograph.test.base.FailOnAllEdgesQuery;
import org.chronos.chronograph.test.base.FailOnAllVerticesQuery;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class GremlinOrTest extends AllChronoGraphBackendsTest {

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canPerformLeadingOrQuery(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("firstname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("lastname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "firstname", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "firstname", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "firstname", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "firstname", "John", "lastname", "Smith");
        g.addVertex(T.id, "5", "firstname", "Jack", "lastname", "Jackson");

        this.assertCommitAssert(() -> {
            // OrStep([[HasStep([firstname.eq(John)])], [HasStep([lastname.eq(Jackson)])]])
            Set<Object> queryResult = g.traversal().V()
                .or(
                    has("firstname", "John"),
                    has("lastname", "Jackson")
                ).id()
                .toSet();
            assertThat(queryResult, containsInAnyOrder("1", "4", "5"));
        });
    }


    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canPerformOrQueryAfterHas(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("firstname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("lastname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "firstname", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "firstname", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "firstname", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "firstname", "John", "lastname", "Smith");
        g.addVertex(T.id, "5", "firstname", "Jack", "lastname", "Jackson");

        this.assertCommitAssert(() -> {
            // OrStep([[HasStep([firstname.eq(John)])], [HasStep([lastname.eq(Jackson)])]])
            Set<Object> queryResult = g.traversal().V()
                .has("lastname", "Doe")
                .or(
                    has("firstname", "John"),
                    has("lastname", "Jackson")
                ).id()
                .toSet();
            assertThat(queryResult, containsInAnyOrder("1"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canPerformInfixOrQuery(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("firstname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("lastname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "firstname", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "firstname", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "firstname", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "firstname", "John", "lastname", "Smith");
        g.addVertex(T.id, "5", "firstname", "Jack", "lastname", "Jackson");
        g.addVertex(T.id, "6", "firstname", "Max", "lastname", "Mustermann");

        this.assertCommitAssert(() -> {
            // [ChronoGraphStep(vertex,[]), OrStep([[HasStep([lastname.eq(Doe)])], [HasStep([firstname.eq(John)]), IdStep]])]
            Set<String> queryResult = g.traversal().V()
                .has("lastname", "Doe")
                .or()
                .has("firstname", "John")
                .or()
                .has("lastname", "Jackson")
                .toStream()
                .map(v -> (String)v.id())
                .collect(Collectors.toSet());
            assertThat(queryResult, containsInAnyOrder("1", "2", "3", "4", "5"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canPerformOrPredicateQuery(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("firstname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("lastname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "firstname", "John", "lastname", "Doe");
        g.addVertex(T.id, "2", "firstname", "Jane", "lastname", "Doe");
        g.addVertex(T.id, "3", "firstname", "Sarah", "lastname", "Doe");
        g.addVertex(T.id, "4", "firstname", "John", "lastname", "Smith");
        g.addVertex(T.id, "5", "firstname", "Jack", "lastname", "Jackson");

        this.assertCommitAssert(() -> {
            // [ChronoGraphStep(vertex,[]), HasStep([lastname.or(eq(Doe), eq(Smith))]), IdStep]
            Set<Object> queryResult = g.traversal().V()
                .has("lastname", P.eq("Doe").or(P.eq("Smith")))
                .id()
                .toSet();
            assertThat(queryResult, containsInAnyOrder("1", "2", "3", "4"));
        });
    }

    @Test
    @FailOnAllVerticesQuery
    @FailOnAllEdgesQuery
    public void canReorderIndexSearchSteps(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("firstname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("lastname").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        g.addVertex(T.id, "1", "firstname", "John", "lastname", "Doe", "age", 35);
        g.addVertex(T.id, "2", "firstname", "Jane", "lastname", "Doe", "age", 32);
        g.addVertex(T.id, "3", "firstname", "Sarah", "lastname", "Doe", "age", 18);
        g.addVertex(T.id, "4", "firstname", "John", "lastname", "Smith", "age", 37);
        g.addVertex(T.id, "5", "firstname", "Jack", "lastname", "Jackson", "age", 38);

        this.assertCommitAssert(() -> {
            Set<Object> queryResult = g.traversal().V()
                // age is not indexed (on purpose)
                .has("age", P.gt(20))
                // but this OR query is!
                .or(
                    has("lastname", "Doe"),
                    has("firstname", "John")
                ).id().toSet();
            assertThat(queryResult, containsInAnyOrder("1", "2", "4"));
        });
    }

    @Test
    public void canHaveLabelsInQuery(){
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("name").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("kind").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onEdgeProperty("associationClass").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().create().stringIndex().onEdgeProperty("kind").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        Vertex a1 = g.addVertex("name", "A1", "kind", "entity");
        Vertex a2 = g.addVertex("name", "A2", "kind", "entity");
        Vertex b1 = g.addVertex("name", "B1", "kind", "entity");

        Edge a1UsesB1 = a1.addEdge("uses", b1);
        a1UsesB1.property("kind", "assocEdge");
        a1UsesB1.property("associationClass", "t:dd2da55d-ad7a-4d6a-b382-19eea2581502");

        Edge a2UsesB1 = a2.addEdge("uses", b1);
        a2UsesB1.property("kind", "assocEdge");
        a2UsesB1.property("associationClass", "t:dd2da55d-ad7a-4d6a-b382-19eea2581502");

        final String LABEL_ASSET = "ASSET";
        final String LABEL_LINK = "LINK";

        GraphTraversal<Edge, Map<String, Object>> result = g.traversal().E()
            .has("kind", "assocEdge")
            .has("associationClass", P.within("t:dd2da55d-ad7a-4d6a-b382-19eea2581502"))
            .as(LABEL_LINK)
            .inV()
            .has("name")
            .as(LABEL_ASSET)
            .select(LABEL_LINK, LABEL_ASSET);

        assertTrue(result.hasNext());
    }
}