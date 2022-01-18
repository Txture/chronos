package org.chronos.chronodb.internal.api.query.searchspec;

import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.internal.impl.query.ContainmentDoubleSearchSpecificationImpl;

import java.util.Set;

public interface ContainmentDoubleSearchSpecification extends ContainmentSearchSpecification<Double> {

    // =================================================================================================================
    // STATIC FACTORY METHODS
    // =================================================================================================================

    static ContainmentDoubleSearchSpecification create(SecondaryIndex index, DoubleContainmentCondition condition, Set<Double> comparisonValues, double equalityTolerance) {
        return new ContainmentDoubleSearchSpecificationImpl(index, condition, comparisonValues, equalityTolerance);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public DoubleContainmentCondition getCondition();

    public double getEqualityTolerance();

    @Override
    public default boolean matches(final Double value){
        return this.getCondition().applies(value, getSearchValue(), getEqualityTolerance());
    }

    @Override
    public default String getDescriptiveSearchType() {
        return "Double";
    }
}
