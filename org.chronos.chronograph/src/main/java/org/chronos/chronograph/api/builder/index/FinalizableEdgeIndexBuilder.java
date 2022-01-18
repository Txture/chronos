package org.chronos.chronograph.api.builder.index;

import org.chronos.chronograph.api.index.ChronoGraphIndex;

public interface FinalizableEdgeIndexBuilder {

    public FinalizableEdgeIndexBuilder assumeNoPriorValues(boolean assumeNoPriorValues);

    public ChronoGraphIndex build();

}
