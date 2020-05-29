package org.chronos.chronograph.api.transaction.trigger;

public interface ChronoGraphPostCommitTrigger extends ChronoGraphTrigger {

    public void onPostCommit(PostCommitTriggerContext context);

}
