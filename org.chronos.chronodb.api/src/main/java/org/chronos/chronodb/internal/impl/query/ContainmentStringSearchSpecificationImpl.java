package org.chronos.chronodb.internal.impl.query;

import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentDoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentStringSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Set;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.*;

public class ContainmentStringSearchSpecificationImpl extends AbstractSearchSpecification<String, StringContainmentCondition, Set<String>> implements ContainmentStringSearchSpecification {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final TextMatchMode matchMode;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public ContainmentStringSearchSpecificationImpl(final String property, final StringContainmentCondition condition, final Set<String> searchValue, TextMatchMode matchMode) {
        super(property, condition, searchValue);
        checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
        this.matchMode = matchMode;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================


    @Override
    public TextMatchMode getMatchMode() {
        return this.matchMode;
    }

    @Override
    public Predicate<Object> toFilterPredicate() {
        return obj -> {
            if (obj instanceof String == false) {
                return false;
            }
            String value = (String) obj;
            return this.condition.applies(value, this.searchValue, this.matchMode);
        };
    }

    @Override
    public SearchSpecification<String, Set<String>> negate() {
        return new ContainmentStringSearchSpecificationImpl(this.property, this.condition.negate(), this.searchValue, this.matchMode);
    }

    @Override
    public String toString() {
        return this.getProperty() + " " + this.getCondition().getInfix() + " " + this.getMatchMode() + " '" + this.getSearchValue() + "'";
    }
}
