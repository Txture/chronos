package org.chronos.chronodb.internal.api.index;

import org.chronos.chronodb.api.IndexManager;
import org.chronos.chronodb.internal.impl.index.querycache.ChronoIndexQueryCache;

public interface IndexManagerInternal extends IndexManager {

    public ChronoIndexQueryCache getIndexQueryCache();

    public void markAllIndicesAsDirty();

}
