package org.chronos.chronodb.internal.api.query.searchspec;

import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.impl.query.ContainmentLongSearchSpecificationImpl;
import org.chronos.chronodb.internal.impl.query.ContainmentStringSearchSpecificationImpl;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

import java.util.Set;

public interface ContainmentLongSearchSpecification extends ContainmentSearchSpecification<Long> {

    // =================================================================================================================
    // STATIC FACTORY METHODS
    // =================================================================================================================

    static ContainmentLongSearchSpecification create(String property, LongContainmentCondition condition, Set<Long> searchValues) {
        return new ContainmentLongSearchSpecificationImpl(property, condition, searchValues);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public LongContainmentCondition getCondition();

    @Override
    public default boolean matches(final Long value){
        return this.getCondition().applies(value, getSearchValue());
    }

    @Override
    public default String getDescriptiveSearchType() {
        return "Long";
    }
}
