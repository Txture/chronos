package org.chronos.chronograph.api.transaction.trigger;

public interface ChronoGraphPreCommitTrigger extends ChronoGraphTrigger {

    public void onPreCommit(PreCommitTriggerContext context) throws CancelCommitException;

}
