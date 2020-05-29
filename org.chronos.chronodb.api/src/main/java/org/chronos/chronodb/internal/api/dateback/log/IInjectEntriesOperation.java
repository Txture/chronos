package org.chronos.chronodb.internal.api.dateback.log;

public interface IInjectEntriesOperation extends DatebackOperation {

    long getOperationTimestamp();

    boolean isCommitMetadataOverride();

}
