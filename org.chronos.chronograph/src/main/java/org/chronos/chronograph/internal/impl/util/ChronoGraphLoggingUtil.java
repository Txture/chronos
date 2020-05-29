package org.chronos.chronograph.internal.impl.util;

import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;

public class ChronoGraphLoggingUtil {

    public static String createLogHeader(ChronoGraphTransaction tx) {
        return "[GRAPH MODIFICATION] :: Coords [" + tx.getBranchName() + "@" + tx.getTimestamp() + "] TxID " + tx.getTransactionId() + " Time " + System.currentTimeMillis() + " :: ";
    }

}
