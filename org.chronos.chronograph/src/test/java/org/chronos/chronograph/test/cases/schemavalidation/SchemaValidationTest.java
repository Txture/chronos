package org.chronos.chronograph.test.cases.schemavalidation;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.exceptions.ChronoGraphSchemaViolationException;
import org.chronos.chronograph.api.schema.ChronoGraphSchemaManager;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.test.base.AllChronoGraphBackendsTest;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class SchemaValidationTest extends AllChronoGraphBackendsTest {

    @Test
    public void canDefineSchemaValidator() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphSchemaManager schemaManager = graph.getSchemaManager();
        boolean override = schemaManager.addOrOverrideValidator("validator1", "return;");
        assertThat(override, is(false));
        boolean override2 = schemaManager.addOrOverrideValidator("validator2", "def x = 1; return;");
        assertThat(override2, is(false));
        String script = schemaManager.getValidatorScript("validator1");
        assertThat(script, is("return;"));
        String script2 = schemaManager.getValidatorScript("validator2");
        assertThat(script2, is("def x = 1; return;"));
        Set<String> allValidatorNames = schemaManager.getAllValidatorNames();
        assertThat(allValidatorNames, containsInAnyOrder("validator1", "validator2"));
    }

    @Test
    public void canOverrideSchemaValidator() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphSchemaManager schemaManager = graph.getSchemaManager();
        boolean override = schemaManager.addOrOverrideValidator("validator", "return;");
        assertThat(override, is(false));
        assertThat(schemaManager.getValidatorScript("validator"), is("return;"));
        boolean override2 = schemaManager.addOrOverrideValidator("validator", "def x = 1; return;");
        assertThat(override2, is(true));
        assertThat(schemaManager.getValidatorScript("validator"), is("def x = 1; return;"));
    }

    @Test
    public void canDeleteSchemaValidator() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphSchemaManager schemaManager = graph.getSchemaManager();
        schemaManager.addOrOverrideValidator("validator", "return");
        assertThat(schemaManager.getValidatorScript("validator"), is("return"));
        boolean removed = schemaManager.removeValidator("validator");
        assertTrue(removed);
        assertThat(schemaManager.getValidatorScript("validator"), is(nullValue()));
        assertThat(schemaManager.removeValidator("validator"), is(false));
    }

    @Test
    public void invalidGroovyScriptsAreRejectedAsArguments() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphSchemaManager schemaManager = graph.getSchemaManager();
        schemaManager.addOrOverrideValidator("validator", "return");
        assertThat(schemaManager.getValidatorScript("validator"), is("return"));
        try {
            schemaManager.addOrOverrideValidator("validator", "this isn't valid groovy!");
            fail("Managed to add schema validator with invalid groovy script body!");
        } catch (IllegalArgumentException expected) {
            // pass
        }
        // assert that the previous validator is still present
        assertThat(schemaManager.getValidatorScript("validator"), is("return"));
    }

    @Test
    public void canPerformSchemaValidation() {
        ChronoGraph graph = this.getGraph();
        ChronoGraphSchemaManager schemaManager = graph.getSchemaManager();
        String validatorName = "All Vertices must have a name";
        schemaManager.addOrOverrideValidator(validatorName, "" +
            "if(element instanceof Vertex){ " +
            "    def v = (Vertex)element; " +
            "    if(!v.property('name').isPresent()){ " +
            "        throw new ChronoGraphSchemaViolationException('The Vertex ' + v.id() + ' has no \"name\" property!');" +
            "    }" +
            "}"
        );
        { // PART 1: attempt at adding an element that doesn't pass the validator
            // try to commit a vertex without a name
            graph.tx().open();
            graph.addVertex();
            try {
                graph.tx().commit();
                fail("Managed to bypass the graph validator!");
            } catch (ChronoGraphSchemaViolationException expected) {
                // pass
            }
            // a failing commit should have performed a rollback
            assertThat(graph.tx().isOpen(), is(false));
            // ... and no vertices should have been written to disk
            graph.tx().open();
            assertThat(graph.vertices().hasNext(), is(false));
            graph.tx().rollback();
        }
        { // PART 2: adding an element which passes the validator
            graph.tx().open();
            // add a valid vertex
            Vertex vertex = graph.addVertex();
            vertex.property("name", "John");
            // commit it (which should be ok according to the validator)
            graph.tx().commit();
        }

        { // PART 3: attempt to commit a mixture of valid and invalid elements
            graph.tx().open();
            Vertex v1 = graph.addVertex();
            Vertex v2 = graph.addVertex();
            v2.property("name", "Jack");
            Vertex v3 = graph.addVertex();
            v3.property("name", "Sarah");
            try {
                graph.tx().commit();
                fail("Managed to perform a commit with graph schema validation errors!");
            } catch (ChronoGraphSchemaViolationException expected) {
                // pass
            }
        }

        { // PART 4: If we remove the validator again, then we can commit whatever we want
            boolean removed = schemaManager.removeValidator(validatorName);
            assertThat(removed, is(true));
            graph.tx().open();
            graph.addVertex();
            graph.tx().commit();
        }

    }

}
