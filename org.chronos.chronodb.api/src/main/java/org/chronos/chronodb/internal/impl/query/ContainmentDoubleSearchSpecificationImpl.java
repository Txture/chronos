package org.chronos.chronodb.internal.impl.query;

import com.google.common.base.Preconditions;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentDoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Objects;
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

    public ContainmentDoubleSearchSpecificationImpl(final SecondaryIndex index, final DoubleContainmentCondition condition, final Set<Double> searchValue, double equalityTolerance) {
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
        return new ContainmentDoubleSearchSpecificationImpl(this.getIndex(), this.condition.negate(), this.searchValue, this.equalityTolerance);
    }

    @Override
    protected Class<Double> getElementValueClass() {
        return Double.class;
    }

    @Override
    public SearchSpecification<Double, Set<Double>> onIndex(final SecondaryIndex index) {
        Preconditions.checkArgument(
            Objects.equals(index.getName(), this.index.getName()),
            "Cannot move search specification on the given index - the index names do not match!"
        );
        return new ContainmentDoubleSearchSpecificationImpl(index, this.condition, this.searchValue, this.equalityTolerance);
    }

    // =================================================================================================================
    // HASHCODE, EQUALS
    // =================================================================================================================

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        ContainmentDoubleSearchSpecificationImpl that = (ContainmentDoubleSearchSpecificationImpl) o;

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
