package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.chronos.chronograph.api.exceptions.GraphTriggerException;
import org.chronos.chronograph.api.transaction.trigger.GraphTriggerMetadata;

public class GraphTriggerMetadataImpl implements GraphTriggerMetadata {

    private final String triggerName;
    private final String triggerClassName;
    private final int priority;
    private final boolean isPreCommmitTrigger;
    private final boolean isPrePersistTrigger;
    private final boolean isPostPersistTrigger;
    private final boolean isPostCommitTrigger;
    private final String userScriptContent;
    private final GraphTriggerException instantiationException;

    public GraphTriggerMetadataImpl(
        final String triggerName,
        final String triggerClassName,
        final int priority,
        final boolean isPreCommmitTrigger,
        final boolean isPrePersistTrigger,
        final boolean isPostPersistTrigger,
        final boolean isPostCommitTrigger,
        final String userScriptContent,
        final GraphTriggerException instantiationException
    ) {
        this.triggerName = triggerName;
        this.triggerClassName = triggerClassName;
        this.priority = priority;
        this.isPreCommmitTrigger = isPreCommmitTrigger;
        this.isPrePersistTrigger = isPrePersistTrigger;
        this.isPostPersistTrigger = isPostPersistTrigger;
        this.isPostCommitTrigger = isPostCommitTrigger;
        this.userScriptContent = userScriptContent;
        this.instantiationException = instantiationException;
    }

    @Override
    public String getTriggerName() {
        return this.triggerName;
    }

    @Override
    public String getTriggerClassName() {
        return this.triggerClassName;
    }

    @Override
    public int getPriority() {
        return this.priority;
    }

    @Override
    public boolean isPreCommitTrigger() {
        return this.isPreCommmitTrigger;
    }

    @Override
    public boolean isPrePersistTrigger() {
        return this.isPrePersistTrigger;
    }

    @Override
    public boolean isPostPersistTrigger() {
        return this.isPostPersistTrigger;
    }

    @Override
    public boolean isPostCommitTrigger() {
        return this.isPostCommitTrigger;
    }

    @Override
    public String getUserScriptContent() {
        return this.userScriptContent;
    }

    @Override
    public GraphTriggerException getInstantiationException() {
        return this.instantiationException;
    }

    @Override
    public String toString() {
        return "GraphTriggerMetadataImpl[" +
            "triggerName='" + triggerName + '\'' +
            ", triggerClassName='" + triggerClassName + '\'' +
            ", priority=" + priority +
            "]";
    }
}
