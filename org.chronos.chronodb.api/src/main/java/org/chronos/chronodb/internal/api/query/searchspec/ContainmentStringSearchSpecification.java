package org.chronos.chronodb.internal.api.query.searchspec;

import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.impl.query.ContainmentStringSearchSpecificationImpl;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

import java.util.Set;

public interface ContainmentStringSearchSpecification extends ContainmentSearchSpecification<String> {

    // =================================================================================================================
    // FACTORY METHODS
    // =================================================================================================================

    public static ContainmentStringSearchSpecification create(final String property, final StringContainmentCondition condition, final TextMatchMode matchMode, final Set<String> searchValues) {
        return new ContainmentStringSearchSpecificationImpl(property, condition, searchValues, matchMode);
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public StringContainmentCondition getCondition();

    public TextMatchMode getMatchMode();

    @Override
    public default boolean matches(final String value){
        return this.getCondition().applies(value, getSearchValue(), getMatchMode());
    }

    @Override
    public default String getDescriptiveSearchType() {
        return "String";
    }
}
