package org.chronos.chronodb.internal.impl.dateback;

import com.google.common.collect.Lists;
import org.chronos.chronodb.api.Branch;
import org.chronos.chronodb.api.Dateback;
import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.api.exceptions.ChronoDBBranchingException;
import org.chronos.chronodb.internal.api.BranchInternal;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.DatebackManagerInternal;
import org.chronos.chronodb.internal.api.dateback.log.DatebackOperation;
import org.chronos.chronodb.internal.api.index.IndexManagerInternal;
import org.chronos.common.autolock.AutoLock;

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractDatebackManager implements DatebackManagerInternal {

    private final ChronoDBInternal db;

    private volatile Thread datebackThread;

    public AbstractDatebackManager(final ChronoDBInternal dbInstance) {
        checkNotNull(dbInstance, "Precondition violation - argument 'dbInstance' must not be NULL!");
        this.db = dbInstance;
    }

    @Override
    public void dateback(final String branch, final Consumer<Dateback> function) {
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        boolean branchExists = this.db.getBranchManager().existsBranch(branch);
        if (branchExists == false) {
            throw new ChronoDBBranchingException("There is no Branch named '" + branch + "'!");
        }
        checkNotNull(function, "Precondition violation - argument 'function' must not be NULL!");
        try (AutoLock lock = this.db.lockExclusive()) {
            this.datebackThread = Thread.currentThread();
            // create the dateback API
            try (DatebackImpl dateback = new DatebackImpl(this.db, branch, this::writeDatebackOperationToLog)) {
                // call the dateback function on the dateback object. It will
                // be closed automatically once the try-block ends, so it cannot
                // be "stolen" by user code.
                function.accept(dateback);
            }
        } finally {
            // clear the dateback thread
            this.datebackThread = null;
            // allow the storage backend to compact the storage (if necessary)
            this.compactStorage(branch);
            // clear all caches
            this.db.getCache().clear();
            this.db.getIndexManager().clearQueryCache();
            // mark all indices as dirty
            IndexManagerInternal indexManager = (IndexManagerInternal)this.db.getIndexManager();
            indexManager.markAllIndicesAsDirty();
        }
    }

    protected void compactStorage(String branch){
        // override in subclasses if necessary
    }

    @Override
    public boolean isDatebackRunning() {
        return this.datebackThread != null;
    }

    @Override
    public void addDatebackOperationToLog(final DatebackOperation operation) {
        checkNotNull(operation, "Precondition violation - argument 'operation' must not be NULL!");
        this.writeDatebackOperationToLog(operation);
    }

    protected ChronoDBInternal getOwningDb(){
        return this.db;
    }

    protected abstract void writeDatebackOperationToLog(DatebackOperation operation);

    @Override
    public List<DatebackOperation> getDatebackOperationsPerformedBetween(String branch, long dateTimeMin, long dateTimeMax){
        checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
        checkArgument(dateTimeMin >= 0, "Precondition violation - argument 'dateTimeMin' must not be negative!");
        checkArgument(dateTimeMax >= 0, "Precondition violation - argument 'dateTimeMax' must not be negative!");
        checkArgument(dateTimeMin <= dateTimeMax, "Precondition violation - argument 'dateTimeMin' must be less than 'dateTimeMax'!");
        BranchInternal b = this.getOwningDb().getBranchManager().getBranch(branch);
        if(b == null){
            throw new IllegalArgumentException("The given branch name '" + branch + "' does not refer to any existing branch!");
        }
        List<Branch> originsRecursive = b.getOriginsRecursive();
        List<DatebackOperation> operations = Lists.newArrayList();
        for(Branch localBranch : originsRecursive){
            operations.addAll(this.getDatebackOperationsPerformedOnBranchBetween(localBranch.getName(), dateTimeMin, dateTimeMax));
        }
        operations.addAll(this.getDatebackOperationsPerformedOnBranchBetween(branch, dateTimeMin, dateTimeMax));
        operations.sort(Comparator.comparing(DatebackOperation::getBranch).thenComparing(DatebackOperation::getWallClockTime));
        return operations;
    }

    protected abstract List<DatebackOperation> getDatebackOperationsPerformedOnBranchBetween(String branch, long dateTimeMin, long dateTimeMax);

    @Override
    public List<DatebackOperation> getDatebackOperationsAffectingTimestamp(String branch, long timestamp) {
        BranchInternal b = this.getOwningDb().getBranchManager().getBranch(branch);
        if(b == null){
            throw new IllegalArgumentException("The given branch name '" + branch + "' does not refer to any existing branch!");
        }
        List<Branch> branchList = Lists.newArrayList(b.getOriginsRecursive());
        branchList.add(b);
        List<DatebackOperation> allOps = this.getAllPerformedDatebackOperations();
        List<DatebackOperation> resultList = Lists.newArrayList();
        for(int i = 0; i < branchList.size(); i++){
            Branch currentBranch = branchList.get(i);
            long affectedTimestamp;
            if(i+1 < branchList.size()){
                affectedTimestamp = branchList.get(i+1).getBranchingTimestamp();
            }else{
                // we are at the end of the parent-branch relationship. The timestamp we need to consider
                // here is our own parameter timestamp.
                affectedTimestamp = timestamp;
            }
            resultList.addAll(allOps.stream()
                .filter(op -> op.getBranch().equals(currentBranch.getName()) && op.affectsTimestamp(affectedTimestamp))
                .collect(Collectors.toList())
            );
        }
        return resultList;
    }

}
