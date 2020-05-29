package org.chronos.chronograph.api.transaction.trigger;

public interface PostTriggerContext extends TriggerContext {

    public long getCommitTimestamp();

    public PreCommitStoreState getPreCommitStoreState();

}
