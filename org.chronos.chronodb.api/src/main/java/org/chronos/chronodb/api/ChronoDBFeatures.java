package org.chronos.chronodb.api;

/**
 * This interface describes the features supported by a {@link ChronoDB} instance.
 *
 * <p>
 * To get an instance of this class, please use {@link ChronoDB#getFeatures()}.
 * </p>
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface ChronoDBFeatures {

    /**
     * Checks if this database is persistent, i.e. has a representation on disk.
     *
     * <p>
     * If this method returns <code>false</code>, the database is in-memory only.
     * </p>
     *
     * @return <code>true</code> if the database is persistent, otherwise <code>false</code>.
     */
    public boolean isPersistent();

    /**
     * Checks if this database supports the rollover mechanism.
     *
     * @return <code>true</code> if rollovers are supported, otherwise <code>false</code>.
     */
    public boolean isRolloverSupported();

    /**
     * Checks if this database supports incremental backups.
     *
     * @return <code>true</code> if incremental backups are supported, otherwise <code>false</code>.
     */
    public boolean isIncrementalBackupSupported();

}
