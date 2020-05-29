package org.chronos.chronodb.internal.api.dateback.log;

public interface IPurgeKeyOperation extends DatebackOperation {

    String getKeyspace();

    String getKey();

    long getFromTimestamp();

    long getToTimestamp();

}
