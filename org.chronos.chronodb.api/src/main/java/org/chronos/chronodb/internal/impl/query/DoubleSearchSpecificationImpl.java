package org.chronos.chronodb.internal.impl.query;

import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.function.Predicate;

public class DoubleSearchSpecificationImpl extends AbstractSearchSpecification<Double, NumberCondition, Double> implements DoubleSearchSpecification {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final double equalityTolerance;

    // =================================================================================================================
    // CONSTRUCTORS
    // =================================================================================================================

    public DoubleSearchSpecificationImpl(final SecondaryIndex index, final NumberCondition condition, final double searchValue, final double equalityTolerance) {
        super(index, condition, searchValue);
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
        return (obj) -> {
            if (obj instanceof Double == false) {
                return false;
            }
            double value = (double) obj;
            return this.condition.applies(value, this.searchValue, this.equalityTolerance);
        };
    }

    @Override
    public SearchSpecification<Double, Double> negate() {
        return new DoubleSearchSpecificationImpl(this.getIndex(), this.getCondition().negate(), this.getSearchValue(), this.getEqualityTolerance());
    }

    @Override
    protected Class<Double> getElementValueClass() {
        return Double.class;
    }

    @Override
    public SearchSpecification<Double, Double> onIndex(final SecondaryIndex index) {
        return new DoubleSearchSpecificationImpl(index, this.condition, this.searchValue, this.equalityTolerance);
    }

    // =================================================================================================================
    // HASH CODE & EQUALS
    // =================================================================================================================

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        DoubleSearchSpecificationImpl that = (DoubleSearchSpecificationImpl) o;

        return Double.compare(that.equalityTolerance, equalityTolerance) == 0;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(equalityTolerance);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
