package org.chronos.chronodb.internal.api;

public interface CommitTimestampProvider {

    public long getNextCommitTimestamp(long nowOnBranch);

}
