package org.chronos.chronograph.api.transaction.trigger;

import org.chronos.chronograph.api.branch.GraphBranch;

public interface TriggerContext extends AutoCloseable {

    public CurrentState getCurrentState();

    public AncestorState getAncestorState();

    public StoreState getStoreState();

    public String getTriggerName();

    public GraphBranch getBranch();

    public Object getCommitMetadata();

    @Override
    void close();
}
