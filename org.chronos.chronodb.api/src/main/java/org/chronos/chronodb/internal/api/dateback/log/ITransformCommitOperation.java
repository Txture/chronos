package org.chronos.chronodb.internal.api.dateback.log;

public interface ITransformCommitOperation extends DatebackOperation {

    long getCommitTimestamp();

}
