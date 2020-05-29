package org.chronos.chronodb.internal.api.dateback.log;

import java.util.Set;

public interface ITransformValuesOperation extends DatebackOperation {

    Set<Long> getCommitTimestamps();

    String getKeyspace();

    String getKey();

}
