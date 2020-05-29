package org.chronos.chronograph.internal.impl.transaction.trigger;

import org.apache.commons.lang3.tuple.Pair;
import org.chronos.chronograph.api.transaction.trigger.*;

import java.util.List;

public interface ChronoGraphTriggerManagerInternal extends ChronoGraphTriggerManager {

    public List<Pair<String, ChronoGraphTrigger>> getAllTriggers();

    public List<Pair<String, ChronoGraphPreCommitTrigger>> getPreCommitTriggers();

    public List<Pair<String, ChronoGraphPrePersistTrigger>> getPrePersistTriggers();

    public List<Pair<String, ChronoGraphPostPersistTrigger>> getPostPersistTriggers();

    public List<Pair<String, ChronoGraphPostCommitTrigger>> getPostCommitTriggers();

}
