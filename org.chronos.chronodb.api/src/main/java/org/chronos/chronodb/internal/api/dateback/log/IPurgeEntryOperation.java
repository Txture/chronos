package org.chronos.chronodb.internal.api.dateback.log;

public interface IPurgeEntryOperation extends DatebackOperation {

    String getKey();

    String getKeyspace();

    long getOperationTimestamp();

}
