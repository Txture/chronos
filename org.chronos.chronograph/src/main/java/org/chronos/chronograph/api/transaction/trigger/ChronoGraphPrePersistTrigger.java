package org.chronos.chronograph.api.transaction.trigger;

public interface ChronoGraphPrePersistTrigger extends ChronoGraphTrigger {

    public void onPrePersist(PrePersistTriggerContext context) throws CancelCommitException;

}
