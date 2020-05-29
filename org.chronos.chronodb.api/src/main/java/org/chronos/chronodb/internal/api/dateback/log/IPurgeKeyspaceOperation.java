package org.chronos.chronodb.internal.api.dateback.log;

public interface IPurgeKeyspaceOperation extends DatebackOperation {

    String getKeyspace();

    long getFromTimestamp();

    long getToTimestamp();

}
