package org.chronos.chronodb.api;

import org.chronos.chronodb.internal.api.dateback.log.DatebackOperation;

import java.util.List;
import java.util.function.Consumer;

public interface DatebackManager {

    /**
     * Executes a set of dateback operations on the {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master}
     * branch.
     *
     * <p>
     * Dateback operations allow to modify the history of the database content. It is <b>strongly discouraged</b> to
     * execute any dateback operation while the database is online and accessible for regular transactions.
     *
     * <p>
     * <u><b>/!\ WARNING /!\</b></u><br>
     * Dateback operations should be a last resort and should be avoided at all costs during regular database operation.
     * They have the following properties:
     * <ul>
     * <li>They are <b>not</b> ACID safe. Back up your database before attempting any dateback operation.
     * <li>They require an <b>exclusive lock</b> on the entire database.
     * <li>They <b>invalidate</b> all currently ongoing transactions. Transactions which are continued after a dateback
     * operation are <b>not guaranteed</b> to be ACID anymore.
     * <li>They <b>cannot</b> be undone.
     * <li>They provide <b>direct access</b> to the temporal stores. There is <b>no safety net</b> to prevent erroneous
     * states.
     * <li>They will always (unconditionally) dirty all secondary indices. Call {@link IndexManager#reindexAll()} after performing your dateback operations.</li>
     * </ul>
     *
     * <b>DO NOT USE THE DATEBACK API UNLESS YOU KNOW <u>EXACTLY</u> WHAT YOU ARE DOING.</b><br>
     *
     * The name of the branch to operate on. Must refer to an existing branch. Must not be <code>null</code>.
     *
     * @param function The function that contains the dateback instructions. Must not be <code>null</code>.
     */
    public default void datebackOnMaster(final Consumer<Dateback> function) {
        this.dateback(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, function);
    }

    /**
     * Executes a set of dateback operations on the given branch.
     *
     * <p>
     * Dateback operations allow to modify the history of the database content. It is <b>strongly discouraged</b> to
     * execute any dateback operation while the database is online and accessible for regular transactions.
     *
     * <p>
     * <u><b>/!\ WARNING /!\</b></u><br>
     * Dateback operations should be a last resort and should be avoided at all costs during regular database operation.
     * They have the following properties:
     * <ul>
     * <li>They are <b>not</b> ACID safe. Back up your database before attempting any dateback operation.
     * <li>They require an <b>exclusive lock</b> on the entire database.
     * <li>They <b>invalidate</b> all currently ongoing transactions. Transactions which are continued after a dateback
     * operation are <b>not guaranteed</b> to be ACID anymore.
     * <li>They <b>cannot</b> be undone.
     * <li>They provide <b>direct access</b> to the temporal stores. There is <b>no safety net</b> to prevent erroneous
     * states.
     * <li>They will always (unconditionally) dirty all secondary indices. Call {@link IndexManager#reindexAll()} after performing your dateback operations.</li>
     * </ul>
     *
     * <b>DO NOT USE THE DATEBACK API UNLESS YOU KNOW <u>EXACTLY</u> WHAT YOU ARE DOING.</b><br>
     *
     * @param branch   The name of the branch to operate on. Must refer to an existing branch. Must not be <code>null</code>.
     * @param function The function that contains the dateback instructions. Must not be <code>null</code>.
     */
    public void dateback(String branch, Consumer<Dateback> function);

    /**
     * Checks if a dateback process is running on the current thread.
     *
     * @return <code>true</code> if a databack process is running, otherwise <code>false</code>.
     */
    public boolean isDatebackRunning();

    /**
     * Returns a list of all dateback operations which have been performed so far on this database instance.
     *
     * @return The list of all performed dateback operations. May be empty, but never <code>null</code>.
     */
    public List<DatebackOperation> getAllPerformedDatebackOperations();

    /**
     * Returns a list of all dateback operations which have been executed within the given time range.
     *
     * <p>
     * The given time range reflects the <b>wall clock time</b> of the operation execution, <i>not</i> the timestamp at which the operation is aimed!
     * </p>
     *
     * @param branch The branch on which the operation has occurred. Operations on origin branches will also be considered.
     * @param dateTimeMin The lower bound of the wall clock time range to search for (inclusive). Must not be negative. Must be less than <code>dateTimeMax</code>.
     * @param dateTimeMax The upper bound of the wall clock time range to search for (inclusive). Must not be negative. Must be greater than <code>dateTimeMin</code>.
     * @return The list of operations on the given <code>branch</code> within the given wall clock time range. May be empty but never <code>null</code>.
     */
    public List<DatebackOperation> getDatebackOperationsPerformedBetween(String branch, long dateTimeMin, long dateTimeMax);

    /**
     * Returns a list of all dateback operations which affect the given <code>branch</code> and <code>timestamp</code>.
     *
     * @param branch The branch on which the operation has occurred. Operations on origin branches will also be considered.
     * @param timestamp The (commit) timestamp in question. All dateback operations aimed at this (or an earlier) timestamp will be returned.
     * @return The list of operations on the given <code>branch</code> which affect the given timestamp. May be empty but never <code>null</code>.
     */
    public List<DatebackOperation> getDatebackOperationsAffectingTimestamp(String branch, long timestamp);
}
