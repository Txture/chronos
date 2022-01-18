package org.chronos.chronograph.api.builder.index;

import org.chronos.chronodb.internal.api.Period;
import org.chronos.chronograph.api.index.ChronoGraphIndex;

/**
 * A step in the fluent graph index builder API.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface VertexIndexBuilder {

    public FinalizableVertexIndexBuilder withValidityPeriod(long startTimestamp, long endTimestamp);

    public default FinalizableVertexIndexBuilder withValidityPeriod(Period period) {
        return this.withValidityPeriod(period.getLowerBound(), period.getUpperBound());
    }

    public default FinalizableVertexIndexBuilder acrossAllTimestamps(){
        return this.withValidityPeriod(Period.eternal());
    }

}
