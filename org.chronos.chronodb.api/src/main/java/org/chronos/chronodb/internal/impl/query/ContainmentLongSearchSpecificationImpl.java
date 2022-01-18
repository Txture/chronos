package org.chronos.chronodb.internal.impl.query;

import com.google.common.base.Preconditions;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentLongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public class ContainmentLongSearchSpecificationImpl extends AbstractSearchSpecification<Long, LongContainmentCondition, Set<Long>> implements ContainmentLongSearchSpecification {

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ContainmentLongSearchSpecificationImpl(final SecondaryIndex index, final LongContainmentCondition condition, final Set<Long> searchValue) {
        super(index, condition, searchValue);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public Predicate<Object> toFilterPredicate() {
        return obj -> {
            if (obj instanceof Long == false) {
                return false;
            }
            long value = (long) obj;
            return this.condition.applies(value, this.searchValue);
        };
    }

    @Override
    public SearchSpecification<Long, Set<Long>> negate() {
        return new ContainmentLongSearchSpecificationImpl(this.getIndex(), this.condition.negate(), this.searchValue);
    }

    @Override
    protected Class<Long> getElementValueClass() {
        return Long.class;
    }

    @Override
    public SearchSpecification<Long, Set<Long>> onIndex(final SecondaryIndex index) {
        Preconditions.checkArgument(
            Objects.equals(index.getName(), this.index.getName()),
            "Cannot move search specification on the given index - the index names do not match!"
        );
        return new ContainmentLongSearchSpecificationImpl(index, this.condition, this.searchValue);
    }

    // =================================================================================================================
    // HASH CODE & EQUALS
    // =================================================================================================================

    @Override
    public int hashCode() {
        return super.hashCode() * 31 * this.getClass().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return this.getClass().equals(obj.getClass());
    }
}
