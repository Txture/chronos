package org.chronos.chronodb.internal.impl.query;

import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentDoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Set;
import java.util.function.Predicate;

public class ContainmentDoubleSearchSpecificationImpl extends AbstractSearchSpecification<Double, DoubleContainmentCondition, Set<Double>> implements ContainmentDoubleSearchSpecification {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final double equalityTolerance;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ContainmentDoubleSearchSpecificationImpl(final String property, final DoubleContainmentCondition condition, final Set<Double> searchValue, double equalityTolerance) {
        super(property, condition, searchValue);
        this.equalityTolerance = equalityTolerance;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public double getEqualityTolerance() {
        return this.equalityTolerance;
    }

    @Override
    public Predicate<Object> toFilterPredicate() {
        return obj -> {
            if(obj instanceof Double == false){
                return false;
            }
            double value = (double)obj;
            return this.condition.applies(value, this.searchValue, this.equalityTolerance);
        };
    }

    @Override
    public SearchSpecification<Double, Set<Double>> negate() {
        return new ContainmentDoubleSearchSpecificationImpl(this.property, this.condition.negate(), this.searchValue, this.equalityTolerance);
    }
}
