package org.chronos.chronodb.internal.impl.dump.meta.dateback;

import static com.google.common.base.Preconditions.*;

public abstract class DatebackLog {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private String id;
    private long wallClockTime;
    private String branch;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected DatebackLog(){
        // default constructor for (de-)serialization
    }

    protected DatebackLog(String id, String branch, long wallClockTime){
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(wallClockTime >= 0, "Precondition violation - argument 'wallClockTime' must not be negative!");
        this.id = id;
        this.branch = branch;
        this.wallClockTime = wallClockTime;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public String getId(){
        return this.id;
    }

    public long getWallClockTime() {
        return this.wallClockTime;
    }

    public String getBranch() {
        return this.branch;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DatebackLog that = (DatebackLog) o;

        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
