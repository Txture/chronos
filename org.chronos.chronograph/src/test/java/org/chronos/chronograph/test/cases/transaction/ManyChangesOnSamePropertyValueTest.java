package org.chronos.chronograph.test.cases.transaction;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Table;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoProperty;
import org.chronos.chronograph.internal.impl.transaction.GraphTransactionContextImpl;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ManyChangesOnSamePropertyValueTest extends AllChronoGraphBackendsTest {

    @Test
    public void overwriteSamePropertyOneMillionTimes() throws Exception {
        ChronoGraph graph = this.getGraph();
        graph.tx().open();
        Vertex v = graph.addVertex();
        for (int i = 0; i < 1_000_000; i++) {
            v.property("i").remove();
            v.property("i", i);
        }
        assertThatOnlyOnePropertyIsModified(graph);

        graph.tx().commit();

        graph.tx().open();
        Vertex v1 = Iterators.getOnlyElement(graph.vertices());
        for (int i = 0; i < 1_000_000; i++) {
            v1.property("i", i);
        }
        this.assertThatOnlyOnePropertyIsModified(graph);
        graph.tx().commit();
    }

    private void assertThatOnlyOnePropertyIsModified(final ChronoGraph graph) throws NoSuchFieldException, IllegalAccessException {
        GraphTransactionContextImpl ctx  = (GraphTransactionContextImpl) graph.tx().getCurrentTransaction().getContext();
        Field field = ctx.getClass().getDeclaredField("vertexPropertyNameToOwnerIdToModifiedProperty");
        field.setAccessible(true);
        Table<String, String, ChronoProperty<?>> table = (Table<String, String, ChronoProperty<?>>)field.get(ctx);
        assertThat(table.size(), is(1));
    }

    @Test
    public void removingPropertyWorksWithIndexing(){
        ChronoGraph graph = this.getGraph();
        graph.getIndexManagerOnMaster().create().stringIndex().onVertexProperty("p").acrossAllTimestamps().build();
        {
            graph.tx().open();
            Vertex v = graph.addVertex();
            v.property("p", "Hello");
            graph.tx().commit();
        }

        {
            graph.tx().open();
            Set<Vertex> queryResult = graph.traversal().V().has("p", "Hello").toSet();
            assertThat(queryResult.size(), is(1));

            Vertex v = Iterables.getOnlyElement(queryResult);
            v.property("p").remove();

            Set<Vertex> queryResult2 = graph.traversal().V().has("p", "Hello").toSet();
            assertThat(queryResult2.size(), is(0));

            graph.tx().commit();
        }

        {
            graph.tx().open();
            Set<Vertex> queryResult = graph.traversal().V().has("p", "Hello").toSet();
            assertThat(queryResult.size(), is(0));
        }


    }

}
