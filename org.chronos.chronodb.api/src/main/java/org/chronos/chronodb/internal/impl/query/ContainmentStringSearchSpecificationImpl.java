package org.chronos.chronodb.internal.impl.query;

import com.google.common.base.Preconditions;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentDoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentStringSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Objects;
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

    public ContainmentStringSearchSpecificationImpl(final SecondaryIndex index, final StringContainmentCondition condition, final Set<String> searchValue, TextMatchMode matchMode) {
        super(index, condition, searchValue);
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
    protected Class<String> getElementValueClass() {
        return String.class;
    }

    @Override
    public SearchSpecification<String, Set<String>> negate() {
        return new ContainmentStringSearchSpecificationImpl(this.getIndex(), this.condition.negate(), this.searchValue, this.matchMode);
    }

    @Override
    public SearchSpecification<String, Set<String>> onIndex(final SecondaryIndex index) {
        Preconditions.checkArgument(
            Objects.equals(index.getName(), this.index.getName()),
            "Cannot move search specification on the given index - the index names do not match!"
        );
        return new ContainmentStringSearchSpecificationImpl(index, this.condition, this.searchValue, this.matchMode);
    }

    // =================================================================================================================
    // HASH CODE, EQUALS, TOSTRING
    // =================================================================================================================

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        ContainmentStringSearchSpecificationImpl that = (ContainmentStringSearchSpecificationImpl) o;

        return matchMode == that.matchMode;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (matchMode != null ? matchMode.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return this.getIndex() + " " + this.getCondition().getInfix() + " " + this.getMatchMode() + " '" + this.getSearchValue() + "'";
    }
}
