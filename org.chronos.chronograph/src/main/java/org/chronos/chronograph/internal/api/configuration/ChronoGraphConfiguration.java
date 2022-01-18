package org.chronos.chronograph.internal.api.configuration;

import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.AllEdgesIterationHandler;
import org.chronos.chronograph.api.transaction.AllVerticesIterationHandler;
import org.chronos.common.configuration.ChronosConfiguration;

/**
 * This class represents the configuration of a single {@link ChronoGraph} instance.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphConfiguration extends ChronosConfiguration {

    // =====================================================================================================================
    // STATIC KEY NAMES
    // =====================================================================================================================

    public static final String NAMESPACE = "org.chronos.chronograph";
    public static final String NS_DOT = NAMESPACE + '.';

    public static final String TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD = NS_DOT + "transaction.checkIdExistenceOnAdd";
    public static final String TRANSACTION_AUTO_OPEN = NS_DOT + "transaction.autoOpen";
    public static final String TRANSACTION_CHECK_GRAPH_INVARIANT = NS_DOT + "transaction.checkGraphInvariant";
    public static final String GRAPH_MODIFICATION_LOG_LEVEL = NS_DOT + "transaction.graphModificationLogLevel";
    public static final String ALL_VERTICES_ITERATION_HANDLER_CLASS_NAME = NS_DOT + "transaction.allVerticesQueryHandlerClassName";
    public static final String ALL_EDGES_ITERATION_HANDLER_CLASS_NAME = NS_DOT + "transaction.allEdgesQueryHandlerClassName";
    public static final String USE_STATIC_GROOVY_COMPILATION_CACHE = NS_DOT + "groovy.cache.useStatic";

    /**
     * Enables or disables utilization of secondary indices for the Gremlin {@link PropertyMapStep valueMap()} step.
     *
     * Please note that this has some side effects:
     * <ul>
     *   <li>The values will be reported as a set, i.e. they will not contain duplicates, and they will be reported in no particular order.</li>
     *   <li>Even if your original property value was a single value (e.g. a single string), it will be reported as a set (of size 1).</li>
     *   <li>If there is a secondary index available, property values that do not match the type of the index will NOT be returned.</li>
     * </ul>
     *
     * <p>
     * Use this setting only if all property values match the type of the index. If that isn't the case, index queries will produce wrong results
     * without warning.
     * </p>
     *
     * <p>
     * The setting can be declared globally in the graph configuration (default: <code>false</code>), or overwritten on a per-traversal basis by calling:
     * </p>
     *
     * <pre>
     * graph.traversal()
     *      .with(ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP, false) // true to enable, false to disable
     *      .V()
     *      ...
     * </pre>
     *
     * @param useSecondaryIndexForGremlinValueMapStep Use <code>true</code> if secondary indices should be used for {@link PropertyMapStep valueMap()} steps.
     *                                              Please see the consequences listed above.
     * @return <code>this</code>, for method chaining.
     */
    public static final String USE_SECONDARY_INDEX_FOR_VALUE_MAP_STEP = NS_DOT + "gremlin.useSecondaryIndexForValueMapStep";

    /**
     * Enables or disables utilization of secondary indices for the Gremlin {@link PropertiesStep values()} step.
     *
     * Please note that this has some side effects:
     * <ul>
     *   <li>The values will be reported as a set, i.e. they will not contain duplicates, and they will be reported in no particular order.</li>
     *   <li>If a property contains multiple values, those values will be flattened.</li>
     *   <li>If there is a secondary index available, property values that do not match the type of the index will NOT be returned.</li>
     * </ul>
     *
     * Use this setting only if all property values match the type of the index. If that isn't the case, index queries will produce wrong results
     * without warning.
     *
     * The setting can be declared globally in the graph configuration (default: <code>false</code>), or overwritten on a per-traversal basis by calling:
     *
     * <pre>
     * graph.traversal()
     *      .with(ChronoGraphConfiguration.USE_SECONDARY_INDEX_FOR_VALUES_STEP, false) // true to enable, false to disable
     *      .V()
     *      ...
     * </pre>
     *
     * @param useSecondaryIndexForGremlinValuesStep Use <code>true</code> if secondary indices should be used for {@link PropertiesStep values()} steps.
     *                                              Please see the consequences listed above.
     * @return <code>this</code>, for method chaining.
     */
    public static final String USE_SECONDARY_INDEX_FOR_VALUES_STEP = NS_DOT + "gremlin.useSecondaryIndexForValuesStep";

    // =================================================================================================================
    // GENERAL CONFIGURATION
    // =================================================================================================================

    /**
     * Checks if graph {@link Element} IDs provided by the user should be checked in the backend for duplicates or not.
     * <p>
     * <p>
     * This property has the following implications:
     * <ul>
     * <li><b>When it is <code>true</code>:</b><br>
     * This is the "trusted" mode. The user of the API will be responsible for providing unique identifiers for the
     * graph elements. No additional checking will be performed before the ID is being used to instantiate a graph
     * element. This mode allows for faster graph element creation, but inconsistencies may be introduced if the user
     * provides duplicate IDs.
     * <li><b>When it is <code>false</code>:</b><br>
     * This is the "untrusted" mode. Before using a user-provided ID for a new graph element, a check will be performed
     * in the underlying persistence if an element with that ID already exists. If such an element already exists, an
     * exception will be thrown and the ID will not be used. This mode is slower when creating new graph elements due to
     * the additional check, but it is also safer in that it warns the user early that a duplicate ID exists.
     * </ul>
     * <p>
     * Regardless whether this setting is on or off, when the user provides no custom ID for a graph element, a new
     * UUID-based identifier will be generated automatically.
     *
     * @return <code>true</code> if a check for duplicated IDs should be performed, or <code>false</code> if that check
     * should be skipped.
     */
    public boolean isCheckIdExistenceOnAddEnabled();

    /**
     * Checks if auto-opening of graph transactions is enabled or not.
     *
     * @return <code>true</code> if auto-opening of graph transactions is enabled, otherwise <code>false</code>.
     */
    public boolean isTransactionAutoOpenEnabled();

    /**
     * Whether or not to perform a check on graph integrity on the change set before committing.
     *
     * @return <code>true</code> if the integrity check should be performed, otherwise <code>false</code>.
     */
    public boolean isGraphInvariantCheckActive();

    /**
     * Returns the handler which should be invoked when a query requires iteration over all vertices.
     *
     * @return The all-vertices-iteration handler. May be <code>null</code>.
     */
    public AllVerticesIterationHandler getAllVerticesIterationHandler();

    /**
     * Returns the handler which should be invoked when a query requires iteration over all edges.
     *
     * @return The all-edges-iteration handler. May be <code>null</code>.
     */
    public AllEdgesIterationHandler getAllEdgesIterationHandler();

    /**
     * Returns <code>true</code> if the static groovy compilation cache should be used.
     *
     * If <code>false</code> is returned, each ChronoGraph instance should use a cache local to that instance.
     *
     * @return <code>true</code> if the static cache should be used, <code>false</code> if a local cache should be used.
     */
    public boolean isUseStaticGroovyCompilationCache();

    /**
     * Whether to use secondary indices for evaluating a Gremlin {@link PropertyMapStep valueMap} step.
     *
     * @return <code>true</code> if secondary indices should be used for the <code>valueMap</code> step, otherwise <code>false</code>.
     */
    public boolean isUseSecondaryIndexForGremlinValueMapStep();

    /**
     * Whether to use secondary indices for evaluating a Gremlin {@link PropertiesStep values} step.
     *
     * @return <code>true</code> if secondary indices should be used for the <code>values</code> step, otherwise <code>false</code>.
     */
    public boolean isUseSecondaryIndexForGremlinValuesStep();

}
