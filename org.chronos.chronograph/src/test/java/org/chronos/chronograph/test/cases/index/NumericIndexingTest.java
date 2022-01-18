package org.chronos.chronograph.test.cases.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.index.ChronoGraphIndex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.index.IndexType;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class NumericIndexingTest extends AllChronoGraphBackendsTest {

    @Test
    public void canAddLongIndex() {
        ChronoGraph g = this.getGraph();
        ChronoGraphIndex index = g.getIndexManagerOnMaster().create().longIndex().onVertexProperty("number").acrossAllTimestamps().build();
        assertNotNull(index);
        g.getIndexManagerOnMaster().reindexAll();
        assertEquals(IndexType.LONG, index.getIndexType());
        assertEquals("number", index.getIndexedProperty());
        assertTrue(g.getIndexManagerOnMaster().getIndexedVertexPropertiesAtAnyPointInTime().contains(index));
    }

    @Test
    public void canAddDoubleIndex() {
        ChronoGraph g = this.getGraph();
        ChronoGraphIndex index = g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("number").acrossAllTimestamps().build();
        assertNotNull(index);
        g.getIndexManagerOnMaster().reindexAll();
        assertEquals(IndexType.DOUBLE, index.getIndexType());
        assertEquals("number", index.getIndexedProperty());
        assertTrue(g.getIndexManagerOnMaster().getIndexedVertexPropertiesAtAnyPointInTime().contains(index));
    }

    @Test
    public void simpleLongQueryUsingGremlinWorks() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().longIndex().onVertexProperty("number").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        { // insert some data
            g.addVertex("name", "a", "number", 24);
            g.addVertex("name", "b", "number", 37);
            g.addVertex("name", "c", "number", 58);
        }
        this.assertCommitAssert(() -> {
            // query the graph
            this.assertNamesEqual("a", g.traversal().V().has("number", P.eq(24)));
            this.assertNamesEqual("a", g.traversal().V().has("number", 24));

            this.assertNamesEqual("c", g.traversal().V().has("number", P.gt(37)));
            this.assertNamesEqual("b", "c", g.traversal().V().has("number", P.gte(37)));

            this.assertNamesEqual("a", "b", g.traversal().V().has("number", P.lt(58)));
            this.assertNamesEqual("a", "b", g.traversal().V().has("number", P.lte(37)));
        });
    }

    @Test
    public void simpleDoubleQueryUsingGremlinWorks() {
        ChronoGraph g = this.getGraph();
        g.getIndexManagerOnMaster().create().doubleIndex().onVertexProperty("number").acrossAllTimestamps().build();
        g.getIndexManagerOnMaster().reindexAll();

        { // insert some data
            g.addVertex("name", "a", "number", 3.1415);
            g.addVertex("name", "b", "number", 24.36);
            g.addVertex("name", "c", "number", 27.58);
        }
        this.assertCommitAssert(() -> {
            // query the graph
            this.assertNamesEqual("a", g.traversal().V().has("number", P.eq(3.1415)));
            this.assertNamesEqual("a", g.traversal().V().has("number", 3.1415));

            this.assertNamesEqual("c", g.traversal().V().has("number", P.gt(24.36)));
            this.assertNamesEqual("b", "c", g.traversal().V().has("number", P.gte(24.36)));

            this.assertNamesEqual("a", "b", g.traversal().V().has("number", P.lt(27.58)));
            this.assertNamesEqual("a", "b", g.traversal().V().has("number", P.lte(24.36)));
        });
    }

    @SuppressWarnings("unchecked")
    private void assertNamesEqual(final Object... objects) {
        List<Object> list = Lists.newArrayList(objects);
        Object last = list.get(list.size() - 1);
        Set<Element> elements = null;
        if (last instanceof Iterable) {
            elements = Sets.newHashSet((Iterable<Element>) last);
        } else if (last instanceof Iterator) {
            elements = Sets.newHashSet((Iterator<Element>) last);
        } else {
            String typeName = "NULL";
            if (last != null) {
                typeName = last.getClass().getName();
            }
            fail("Last element of 'assertNamesEqual' varargs must either be a Iterable<Element> or a Iterator<Element> (found: " + typeName + ")");
        }
        Set<String> elementNames = elements.stream().map(e -> (String) e.value("name")).collect(Collectors.toSet());
        Set<String> names = list.subList(0, list.size() - 1).stream().map(k -> (String) k).collect(Collectors.toSet());
        assertEquals(names, elementNames);
    }

}
