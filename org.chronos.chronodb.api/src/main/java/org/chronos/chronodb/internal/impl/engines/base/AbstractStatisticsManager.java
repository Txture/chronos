package org.chronos.chronodb.internal.impl.engines.base;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.chronos.chronodb.api.BranchHeadStatistics;
import org.chronos.chronodb.api.ChronoDBStatistics;
import org.chronos.chronodb.api.exceptions.ChronoDBException;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.api.StatisticsManagerInternal;
import org.chronos.chronodb.internal.impl.BranchHeadStatisticsImpl;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractStatisticsManager implements StatisticsManagerInternal {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ReadWriteLock statisticsLock = new ReentrantReadWriteLock(true);
    private final LoadingCache<String, BranchHeadStatistics> branchToHeadStatistics;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public AbstractStatisticsManager() {
        this.branchToHeadStatistics = CacheBuilder.newBuilder().build(new CacheLoader<String, BranchHeadStatistics>() {
            @Override
            public BranchHeadStatistics load(final String key) throws Exception {
                return AbstractStatisticsManager.this.loadBranchHeadStatistics(key);
            }
        });
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public ChronoDBStatistics getGlobalStatistics() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public BranchHeadStatistics getBranchHeadStatistics(final String branchName) {
        this.statisticsLock.readLock().lock();
        try {
            return this.branchToHeadStatistics.get(branchName);
        } catch (ExecutionException e) {
            throw new ChronoDBException("Failed to calculate head statistics!", e);
        } finally {
            this.statisticsLock.readLock().unlock();
        }
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    @Override
    public void updateBranchHeadStatistics(String branchName, long inserts, long updates, long deletes) {
        this.statisticsLock.writeLock().lock();
        try {
            Optional<BranchHeadStatistics> maybeHeadStats = Optional.ofNullable(this.branchToHeadStatistics.getIfPresent(branchName));
            if(maybeHeadStats.isPresent()){
                BranchHeadStatistics oldStatistics = maybeHeadStats.get();
                long numberOfEntriesInHead = oldStatistics.getNumberOfEntriesInHead();
                long totalNumberOfEntries = oldStatistics.getTotalNumberOfEntries();

                //changes entries in head
                numberOfEntriesInHead += inserts;
                numberOfEntriesInHead -= deletes;

                //changes total entries
                totalNumberOfEntries += inserts;
                totalNumberOfEntries += deletes;
                totalNumberOfEntries += updates;

                BranchHeadStatistics newStatistics = new BranchHeadStatisticsImpl(numberOfEntriesInHead, totalNumberOfEntries);
                this.branchToHeadStatistics.put(branchName, newStatistics);
                this.saveBranchHeadStatistics(branchName, newStatistics);
            }else{
                // calculate from scratch
                BranchHeadStatistics newStatistics = this.loadBranchHeadStatistics(branchName);
                this.branchToHeadStatistics.put(branchName, newStatistics);
                this.saveBranchHeadStatistics(branchName, newStatistics);
            }
        } finally {
            this.statisticsLock.writeLock().unlock();
        }
    }

    @Override
    public void clearBranchHeadStatistics() {
        this.statisticsLock.writeLock().lock();
        try{
            this.branchToHeadStatistics.invalidateAll();
            this.deleteBranchHeadStatistics();
        }finally{
            this.statisticsLock.writeLock().unlock();
        }
    }

    @Override
    public void clearBranchHeadStatistics(final String branchName) {
        this.statisticsLock.writeLock().lock();
        try{
            this.branchToHeadStatistics.invalidate(branchName);
            this.deleteBranchHeadStatistics(branchName);
        }finally{
            this.statisticsLock.writeLock().unlock();
        }
    }

    // =================================================================================================================
    // ABSTRACT METHODS
    // =================================================================================================================

    protected abstract BranchHeadStatistics loadBranchHeadStatistics(String branch);

    protected abstract void saveBranchHeadStatistics(String branchName, BranchHeadStatistics statistics);

    protected abstract void deleteBranchHeadStatistics();

    protected abstract void deleteBranchHeadStatistics(String branchName);


}
