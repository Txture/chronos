package org.chronos.chronodb.internal.impl.query;

import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentLongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Set;
import java.util.function.Predicate;

public class ContainmentLongSearchSpecificationImpl extends AbstractSearchSpecification<Long, LongContainmentCondition, Set<Long>> implements ContainmentLongSearchSpecification {

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ContainmentLongSearchSpecificationImpl(final String property, final LongContainmentCondition condition, final Set<Long> searchValue) {
        super(property, condition, searchValue);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public Predicate<Object> toFilterPredicate() {
        return obj -> {
            if(obj instanceof Long == false){
                return false;
            }
            long value = (long)obj;
            return this.condition.applies(value, this.searchValue);
        };
    }

    @Override
    public SearchSpecification<Long, Set<Long>> negate() {
        return new ContainmentLongSearchSpecificationImpl(this.property, this.condition.negate(), this.searchValue);
    }

}
