package org.chronos.chronograph.internal.impl.configuration;

import org.chronos.chronograph.api.transaction.AllEdgesIterationHandler;
import org.chronos.chronograph.api.transaction.AllVerticesIterationHandler;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.common.configuration.AbstractConfiguration;
import org.chronos.common.configuration.annotation.EnumFactoryMethod;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;
import org.chronos.common.logging.LogLevel;

import javax.annotation.PostConstruct;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

@Namespace(ChronoGraphConfiguration.NAMESPACE)
public class ChronoGraphConfigurationImpl extends AbstractConfiguration implements ChronoGraphConfiguration {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    @Parameter(key = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD)
    private boolean checkIdExistenceOnAdd = true;

    @Parameter(key = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN)
    private boolean txAutoOpenEnabled = true;

    @Parameter(key = ChronoGraphConfiguration.TRANSACTION_CHECK_GRAPH_INVARIANT)
    private boolean graphInvariantCheckActive = false;

    @Parameter(key = ChronoGraphConfiguration.GRAPH_MODIFICATION_LOG_LEVEL)
    @EnumFactoryMethod("fromString")
    private LogLevel graphModificationLogLevel = LogLevel.OFF;

    @Parameter(key = ChronoGraphConfiguration.ALL_VERTICES_ITERATION_HANDLER_CLASS_NAME, optional = true)
    private String allVerticesIterationHandlerClassName = null;

    @Parameter(key = ChronoGraphConfiguration.ALL_EDGES_ITERATION_HANDLER_CLASS_NAME, optional = true)
    private String allEdgesIterationHandlerClassName = null;

    // =================================================================================================================
    // CACHE
    // =================================================================================================================

    private AllVerticesIterationHandler allVerticesIterationHandler;
    private AllEdgesIterationHandler allEdgesIterationHandler;

    // =================================================================================================================
    // INIT
    // =================================================================================================================

    @PostConstruct
    private void init() {
        if (this.allVerticesIterationHandlerClassName != null && this.allVerticesIterationHandlerClassName.trim().isEmpty() == false) {
            Class<?> handlerClass;
            try {
                handlerClass = Class.forName(this.allVerticesIterationHandlerClassName.trim());
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                throw new IllegalArgumentException("Failed to instantiate AllVerticesIterationHandler class '" + this.allVerticesIterationHandlerClassName + "'!", e);
            }
            if (!AllVerticesIterationHandler.class.isAssignableFrom(handlerClass)) {
                throw new IllegalArgumentException("The given AllVerticesIterationHandler class '" + this.allVerticesIterationHandlerClassName + "' is not a subclass of " + AllVerticesIterationHandler.class.getSimpleName() + "!");
            }
            try {
                Constructor<?> constructor = handlerClass.getConstructor();
                constructor.setAccessible(true);
                this.allVerticesIterationHandler = (AllVerticesIterationHandler) constructor.newInstance();
            } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                throw new IllegalArgumentException("Failed to instantiate AllVerticesIterationHandler class '" + this.allVerticesIterationHandlerClassName + "' - does it have a default constructor?", e);
            }
        }
        if(this.allEdgesIterationHandlerClassName != null && this.allEdgesIterationHandlerClassName.trim().isEmpty() == false){
            Class<?> handlerClass;
            try {
                handlerClass = Class.forName(this.allEdgesIterationHandlerClassName.trim());
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                throw new IllegalArgumentException("Failed to instantiate AllEdgesIterationHandler class '" + this.allEdgesIterationHandlerClassName + "'!", e);
            }
            if (!AllEdgesIterationHandler.class.isAssignableFrom(handlerClass)) {
                throw new IllegalArgumentException("The given AllEdgesIterationHandler class '" + this.allEdgesIterationHandlerClassName + "' is not a subclass of " + AllEdgesIterationHandler.class.getSimpleName() + "!");
            }
            try {
                Constructor<?> constructor = handlerClass.getConstructor();
                constructor.setAccessible(true);
                this.allEdgesIterationHandler = (AllEdgesIterationHandler) constructor.newInstance();
            } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                throw new IllegalArgumentException("Failed to instantiate AllEdgesIterationHandler class '" + this.allEdgesIterationHandlerClassName + "' - does it have a default constructor?", e);
            }
        }
    }

    // =================================================================================================================
    // GETTERS
    // =================================================================================================================

    @Override
    public boolean isCheckIdExistenceOnAddEnabled() {
        return this.checkIdExistenceOnAdd;
    }

    @Override
    public boolean isTransactionAutoOpenEnabled() {
        return this.txAutoOpenEnabled;
    }

    @Override
    public boolean isGraphInvariantCheckActive() {
        return this.graphInvariantCheckActive;
    }

    @Override
    public LogLevel getGraphModificationLogLevel() {
        if (this.graphModificationLogLevel == null) {
            return LogLevel.OFF;
        }
        return this.graphModificationLogLevel;
    }

    @Override
    public AllVerticesIterationHandler getAllVerticesIterationHandler() {
        return this.allVerticesIterationHandler;
    }

    @Override
    public AllEdgesIterationHandler getAllEdgesIterationHandler() {
        return this.allEdgesIterationHandler;
    }
}
