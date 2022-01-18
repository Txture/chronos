package org.chronos.chronograph.api.builder.index;

import org.chronos.chronodb.internal.api.Period;

/**
 * A step in the fluent graph index builder API.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 */
public interface EdgeIndexBuilder {

    public FinalizableEdgeIndexBuilder withValidityPeriod(long startTimestamp, long endTimestamp);

    public default FinalizableEdgeIndexBuilder withValidityPeriod(Period period) {
        return this.withValidityPeriod(period.getLowerBound(), period.getUpperBound());
    }

    public default FinalizableEdgeIndexBuilder acrossAllTimestamps(){
        return this.withValidityPeriod(Period.eternal());
    }

}
