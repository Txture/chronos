package org.chronos.chronodb.internal.api;

import org.chronos.chronodb.api.DatebackManager;
import org.chronos.chronodb.internal.api.dateback.log.DatebackOperation;

public interface DatebackManagerInternal extends DatebackManager {

    /**
     * Adds the given operation to the databack log.
     *
     * <p>
     * <b>/!\ For internal purposes only.</b>
     * </p>
     *
     * @param operation The operation to add to the log. Must not be <code>null</code>.
     */
    public void addDatebackOperationToLog(DatebackOperation operation);

    public void deleteLogsForBranch(String branchName);

}
