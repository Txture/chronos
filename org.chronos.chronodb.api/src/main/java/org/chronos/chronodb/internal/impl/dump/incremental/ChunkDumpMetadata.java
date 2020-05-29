package org.chronos.chronodb.internal.impl.dump.incremental;

import com.google.common.collect.Lists;
import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronodb.internal.impl.dump.meta.CommitDumpMetadata;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public class ChunkDumpMetadata {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private String branchName;
    private long chunkSequenceNumber;
    private long validFrom;
    private long validTo;
    private List<CommitDumpMetadata> commitMetadata;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    protected ChunkDumpMetadata(){
        // default constructor for (de-)serialization
    }

    public ChunkDumpMetadata(String branchName, long chunkSequenceNumber, Period validPeriod, List<CommitDumpMetadata> commitMetadata){
        checkNotNull(branchName, "Precondition violation - argument 'branchName' must not be NULL!");
        checkArgument(chunkSequenceNumber >= 0, "Precondition violation - argument 'chunkSequenceNumber' must not be negative!");
        checkNotNull(validPeriod, "Precondition violation - argument 'validPeriod' must not be NULL!");
        checkNotNull(commitMetadata, "Precondition violation - argument 'commitMetadata' must not be NULL!");
        this.branchName = branchName;
        this.chunkSequenceNumber = chunkSequenceNumber;
        this.validFrom = validPeriod.getLowerBound();
        this.validTo = validPeriod.getUpperBound();
        this.commitMetadata = Lists.newArrayList(commitMetadata);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    public String getBranchName() {
        return branchName;
    }

    public long getChunkSequenceNumber() {
        return chunkSequenceNumber;
    }

    public Period getValidPeriod(){
        return Period.createRange(this.validFrom, this.validTo);
    }

    public List<CommitDumpMetadata> getCommitMetadata() {
        return Collections.unmodifiableList(commitMetadata);
    }
}
