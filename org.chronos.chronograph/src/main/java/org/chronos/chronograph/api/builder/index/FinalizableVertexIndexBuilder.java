package org.chronos.chronograph.api.builder.index;

import org.chronos.chronograph.api.index.ChronoGraphIndex;

public interface FinalizableVertexIndexBuilder {

    public FinalizableVertexIndexBuilder assumeNoPriorValues(boolean assumeNoPriorValues);

    public ChronoGraphIndex build();

}
