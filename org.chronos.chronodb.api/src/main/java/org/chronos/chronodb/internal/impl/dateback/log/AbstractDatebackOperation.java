package org.chronos.chronodb.internal.impl.dateback.log;

import org.chronos.chronodb.internal.api.dateback.log.DatebackOperation;

import java.util.UUID;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractDatebackOperation implements DatebackOperation {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private String id;
    private long wallClockTime;
    private String branch;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    protected AbstractDatebackOperation(){
        // default constructor for (de-)serialization
    }

    protected AbstractDatebackOperation(String id, String branch, long wallClockTime){
        checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(wallClockTime >= 0, "Precondition violation - argument 'wallClockTime' must not be negative!");
        this.id = id;
        this.branch = branch;
        this.wallClockTime = wallClockTime;
    }

    protected AbstractDatebackOperation(String branch){
        this(UUID.randomUUID().toString(), branch, System.currentTimeMillis());
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public String getId(){
        return this.id;
    }

    @Override
    public long getWallClockTime() {
        return this.wallClockTime;
    }

    @Override
    public String getBranch() {
        return this.branch;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AbstractDatebackOperation that = (AbstractDatebackOperation) o;

        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
