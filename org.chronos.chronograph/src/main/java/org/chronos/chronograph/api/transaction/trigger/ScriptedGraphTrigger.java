package org.chronos.chronograph.api.transaction.trigger;

import org.chronos.chronograph.internal.impl.transaction.trigger.script.ScriptedGraphPostCommitTrigger;
import org.chronos.chronograph.internal.impl.transaction.trigger.script.ScriptedGraphPostPersistTrigger;
import org.chronos.chronograph.internal.impl.transaction.trigger.script.ScriptedGraphPreCommitTrigger;
import org.chronos.chronograph.internal.impl.transaction.trigger.script.ScriptedGraphPrePersistTrigger;

import static com.google.common.base.Preconditions.*;

public interface ScriptedGraphTrigger {

    public static ChronoGraphPreCommitTrigger createPreCommitTrigger(String scriptContent, int priority){
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        return new ScriptedGraphPreCommitTrigger(scriptContent, priority);
    }

    public static ChronoGraphPrePersistTrigger createPrePersistTrigger(String scriptContent, int priority){
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        return new ScriptedGraphPrePersistTrigger(scriptContent, priority);
    }

    public static ChronoGraphPostPersistTrigger createPostPersistTrigger(String scriptContent, int priority){
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        return new ScriptedGraphPostPersistTrigger(scriptContent, priority);
    }

    public static ChronoGraphPostCommitTrigger createPostCommitTrigger(String scriptContent, int priority){
        checkNotNull(scriptContent, "Precondition violation - argument 'scriptContent' must not be NULL!");
        return new ScriptedGraphPostCommitTrigger(scriptContent, priority);
    }


}
