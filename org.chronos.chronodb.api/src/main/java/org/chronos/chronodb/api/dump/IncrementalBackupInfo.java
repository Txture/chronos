package org.chronos.chronodb.api.dump;

import org.chronos.common.version.ChronosVersion;

import static com.google.common.base.Preconditions.*;

public class IncrementalBackupInfo {

    private ChronosVersion chronosVersion;
    private long dumpFormatVersion;
    private long requestStartTimestamp;
    private long previousRequestWallClockTime;
    private long now;
    private long wallClockTime;

    public IncrementalBackupInfo(final ChronosVersion chronosVersion, final int dumpFormatVersion, final long requestStartTimestamp, final long previousRequestWallClockTime, final long now, final long wallClockTime) {
        checkNotNull(chronosVersion, "Precondition violation - argument 'chronosVersion' must not be NULL!");
        checkArgument(requestStartTimestamp <= now, "Precondition violation - argument 'requestStartTimestamp' must be less than 'now'!");
        checkArgument(previousRequestWallClockTime <= wallClockTime, "Precondition violation - argument 'previousRequestWallClockTime' must be less than 'wallClockTime'!");
        this.chronosVersion = chronosVersion;
        this.dumpFormatVersion = dumpFormatVersion;
        this.requestStartTimestamp = requestStartTimestamp;
        this.previousRequestWallClockTime = previousRequestWallClockTime;
        this.now = now;
        this.wallClockTime = wallClockTime;
    }

    public ChronosVersion getChronosVersion() {
        return chronosVersion;
    }

    public long getDumpFormatVersion() {
        return dumpFormatVersion;
    }

    public long getRequestStartTimestamp() {
        return requestStartTimestamp;
    }

    public long getPreviousRequestWallClockTime() {
        return previousRequestWallClockTime;
    }

    public long getNow() {
        return now;
    }

    public long getWallClockTime() {
        return wallClockTime;
    }

}