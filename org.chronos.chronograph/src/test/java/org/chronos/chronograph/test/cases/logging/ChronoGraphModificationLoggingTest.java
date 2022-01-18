package org.chronos.chronograph.test.cases.logging;

import org.apache.tinkerpop.gremlin.structure.T;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;


@Category(IntegrationTest.class)
public class ChronoGraphModificationLoggingTest extends AllChronoGraphBackendsTest {


    @Test
    public void graphModificationLoggingIsDisabledByDefault() {
        final ByteArrayOutputStream myOut = new ByteArrayOutputStream();
        PrintStream originalSysout = System.out;
        System.setOut(new PrintStream(myOut));
        try {
            ChronoGraph graph = this.getGraph();
            graph.tx().open();
            graph.addVertex(T.id, "1234");
            graph.tx().commit();
        } finally {
            System.setOut(originalSysout);
        }
        final String testOutput = myOut.toString();
        assertThat(testOutput.toLowerCase(), not(containsString("adding vertex")));
    }

    @Test
    @InstantiateChronosWith(property = ChronoGraphConfiguration.GRAPH_MODIFICATION_LOG_LEVEL, value = "info")
    public void canEnableGraphModificationLogging() {
        final ByteArrayOutputStream myOut = new ByteArrayOutputStream();
        PrintStream originalSysout = System.out;
        System.setOut(new PrintStream(myOut));
        try {
            ChronoGraph graph = this.getGraph();
            graph.tx().open();
            graph.addVertex(T.id, "1234", "firstname", "John", "lastname", "Doe");
            graph.tx().commit();
        } finally {
            System.setOut(originalSysout);
        }
        final String testOutput = myOut.toString();
        System.out.println("<<<< BEGIN TEST OUTPUT >>>>");
        System.out.println(testOutput);
        System.out.println("<<<< END TEST OUTPUT >>>>");
        assertThat(testOutput, not(isEmptyString()));
    }

}
