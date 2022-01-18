package org.chronos.chronograph.api.builder.graph;

import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.common.builder.ChronoBuilder;

/**
 * A builder for instances of {@link ChronoGraph}.
 *
 * <p>
 * When an instance of this interface is returned by the fluent builder API, then all information required for building
 * the database is complete, and {@link #build()} can be called to finalize the build process.
 *
 * <p>
 * Even though the {@link #build()} method becomes available at this stage, it is still possible to set properties
 * defined by the concrete implementations.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoGraphFinalizableBuilder extends ChronoBuilder<ChronoGraphFinalizableBuilder> {

    /**
     * Enables or disables the check for ID existence when adding a new graph element with a user-provided ID.
     *
     * <p>
     * For details, please refer to {@link ChronoGraphConfiguration#isCheckIdExistenceOnAddEnabled()}.
     *
     * @param enableIdExistenceCheckOnAdd Use <code>true</code> to enable the safety check, or <code>false</code> to disable it.
     * @return <code>this</code>, for method chaining.
     */
    public ChronoGraphFinalizableBuilder withIdExistenceCheckOnAdd(boolean enableIdExistenceCheckOnAdd);

    /**
     * Enables or disables automatic opening of new graph transactions on-demand.
     *
     * <p>
     * For details, please refer to {@link ChronoGraphConfiguration#isTransactionAutoOpenEnabled()}.
     *
     * @param enableAutoStartTransactions Set this to <code>true</code> if auto-start of transactions should be enabled (default), otherwise set
     *                                    it to <code>false</code> to disable this feature.
     * @return <code>this</code>, for method chaining.
     */
    public ChronoGraphFinalizableBuilder withTransactionAutoStart(boolean enableAutoStartTransactions);

    /**
     * Enables or disables the static (i.e. shared) groovy compilation cache.
     *
     * <p>
     * For details, please refer to {@link ChronoGraphConfiguration#isUseStaticGroovyCompilationCache()}.
     * </p>
     *
     * @param enableStaticGroovyCompilationCache Set this to <code>true</code> to enable the static groovy compilation cache.
     *                                           Use <code>false</code> (default) if each ChronoGraph instance should have its own cache.
     * @return <code>this</code>, for method chaining.
     */
    public ChronoGraphFinalizableBuilder withStaticGroovyCompilationCache(boolean enableStaticGroovyCompilationCache);

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
     * This is the global setting that will affect all traversals performed on this graph. The setting can be
     * overwritten on a per-traversal basis by calling:
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
    public ChronoGraphFinalizableBuilder withUsingSecondaryIndicesForGremlinValuesStep(boolean useSecondaryIndexForGremlinValuesStep);

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
     * This is the global setting that will affect all traversals performed on this graph. The setting can be
     * overwritten on a per-traversal basis by calling:
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
    public ChronoGraphFinalizableBuilder withUsingSecondaryIndicesForGremlinValueMapStep(boolean useSecondaryIndexForGremlinValueMapStep);

    /**
     * Builds the {@link ChronoGraph} instance, using the properties specified by the fluent API.
     *
     * <p>
     * This method finalizes the build process. Afterwards, the builder should be discarded.
     *
     * @return The new {@link ChronoGraph} instance. Never <code>null</code>.
     */
    public ChronoGraph build();

}