package org.chronos.chronograph.test.base;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.chronos.chronodb.test.base.AllBackendsTest;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal;
import org.chronos.chronograph.test.util.FailOnAllEdgesIterationHandler;
import org.chronos.chronograph.test.util.FailOnAllVerticesIterationHandler;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.*;

public abstract class AllChronoGraphBackendsTest extends AllBackendsTest {

    private static final Logger log = LoggerFactory.getLogger(AllChronoGraphBackendsTest.class);

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private ChronoGraph graph;

    // =================================================================================================================
    // GETTERS & SETTERS
    // =================================================================================================================

    protected ChronoGraph getGraph() {
        if (this.graph == null) {
            this.graph = this.instantiateChronoGraph(this.backend);
        }
        return this.graph;
    }

    // =================================================================================================================
    // JUNIT CONTROL
    // =================================================================================================================

    @After
    public void cleanUp() {
        if (this.graph != null && this.graph.isClosed() == false) {
            if (this.graph.tx().isOpen()) {
                this.graph.tx().rollback();
            }
            log.debug("Closing ChronoDB on backend '" + this.backend + "'.");
            this.graph.close();
        }
    }

    // =================================================================================================================
    // UTILITY
    // =================================================================================================================

    protected ChronoGraph reinstantiateGraph() {
        log.debug("Reinstantiating ChronoGraph on backend '" + this.backend + "'.");
        this.graph.close();
        this.graph = this.instantiateChronoGraph(this.backend);
        return this.graph;
    }

    protected ChronoGraph closeAndReopenGraph(){
        return this.closeAndReopenGraph(new BaseConfiguration());
    }

    protected ChronoGraph closeAndReopenGraph(Configuration additionalConfiguration){
        Configuration dbConfig = ((ChronoGraphInternal) this.graph).getBackingDB().getConfiguration().asCommonsConfiguration();
        Configuration graphConfig = this.graph.getChronoGraphConfiguration().asCommonsConfiguration();
        this.graph.close();
        ConfigurationUtils.copy(dbConfig, graphConfig);
        ConfigurationUtils.copy(additionalConfiguration, graphConfig);
        this.graph = ChronoGraph.FACTORY.create().fromConfiguration(graphConfig).build();
        return this.graph;
    }

    protected ChronoGraph instantiateChronoGraph(final String backend) {
        checkNotNull(backend, "Precondition violation - argument 'backend' must not be NULL!");
        Configuration configuration = this.createChronosConfiguration(backend);
        return this.createGraph(configuration);
    }

    protected ChronoGraph createGraph(final Configuration configuration) {
        checkNotNull(configuration, "Precondition violation - argument 'configuration' must not be NULL!");
        this.applyExtraTestMethodProperties(configuration);
        // always enable graph integrity validation for chronograph tests
        configuration.setProperty(ChronoGraphConfiguration.TRANSACTION_CHECK_GRAPH_INVARIANT, "true");
        return ChronoGraph.FACTORY.create().fromConfiguration(configuration).build();
    }

    @Override
    protected void applyExtraTestMethodProperties(final Configuration configuration) {
        super.applyExtraTestMethodProperties(configuration);
        Method currentTestMethod = this.getCurrentTestMethod();
        if(currentTestMethod.getAnnotation(FailOnAllVerticesQuery.class) != null){
            configuration.setProperty(ChronoGraphConfiguration.ALL_VERTICES_ITERATION_HANDLER_CLASS_NAME, FailOnAllVerticesIterationHandler.class.getName());
        }
        if(currentTestMethod.getAnnotation(FailOnAllEdgesQuery.class) != null){
            configuration.setProperty(ChronoGraphConfiguration.ALL_EDGES_ITERATION_HANDLER_CLASS_NAME, FailOnAllEdgesIterationHandler.class.getName());
        }
    }

    protected void assertCommitAssert(final Runnable assertion) {
        assertion.run();
        this.getGraph().tx().commit();
        assertion.run();
    }

}
