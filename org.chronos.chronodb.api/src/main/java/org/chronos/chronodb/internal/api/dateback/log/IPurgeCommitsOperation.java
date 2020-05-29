package org.chronos.chronodb.internal.api.dateback.log;

import java.util.Set;

public interface IPurgeCommitsOperation extends DatebackOperation {

    Set<Long> getCommitTimestamps();

}
