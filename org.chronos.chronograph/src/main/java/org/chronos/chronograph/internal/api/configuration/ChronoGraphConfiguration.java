package org.chronos.chronograph.internal.api.configuration;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.api.structure.ChronoGraph;
import org.chronos.chronograph.api.transaction.AllEdgesIterationHandler;
import org.chronos.chronograph.api.transaction.AllVerticesIterationHandler;
import org.chronos.common.configuration.ChronosConfiguration;
import org.chronos.common.logging.LogLevel;

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
     * Returns the desired log level for graph modifications.
     *
     * @return The graph modification log level.
     */
    public LogLevel getGraphModificationLogLevel();

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
     * Checks whether or not graph modification logging is active at all.
     *
     * @return <code>true</code> if graph modification logging is desired, otherwise <code>false</code>.
     */
    public default boolean isGraphModificationLoggingActive() {
        return this.getGraphModificationLogLevel().isGreaterThan(LogLevel.OFF);
    }
}
