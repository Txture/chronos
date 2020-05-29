package org.chronos.chronograph.api.transaction.trigger;

import org.chronos.chronograph.api.exceptions.GraphTriggerException;

public interface GraphTriggerMetadata {

    public String getTriggerName();

    public String getTriggerClassName();

    public int getPriority();

    public boolean isPreCommitTrigger();

    public boolean isPrePersistTrigger();

    public boolean isPostPersistTrigger();

    public boolean isPostCommitTrigger();

    public String getUserScriptContent();

    public GraphTriggerException getInstantiationException();

}
