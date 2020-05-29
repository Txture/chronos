package org.chronos.chronodb.internal.api.dateback.log;

public interface IUpdateCommitMetadataOperation extends DatebackOperation {

    long getCommitTimestamp();

}
