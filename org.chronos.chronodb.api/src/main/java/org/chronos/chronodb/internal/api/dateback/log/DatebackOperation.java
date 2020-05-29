package org.chronos.chronodb.internal.api.dateback.log;

public interface DatebackOperation {

    public String getId();

    public long getWallClockTime();

    public String getBranch();

    public boolean affectsTimestamp(long timestamp);

    public long getEarliestAffectedTimestamp();
}
