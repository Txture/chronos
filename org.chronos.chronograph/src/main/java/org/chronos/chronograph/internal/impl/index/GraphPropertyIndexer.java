package org.chronos.chronograph.internal.impl.index;

import org.chronos.chronodb.api.indexing.Indexer;

public interface GraphPropertyIndexer<T> extends Indexer<T> {

    public String getGraphElementPropertyName();

}
