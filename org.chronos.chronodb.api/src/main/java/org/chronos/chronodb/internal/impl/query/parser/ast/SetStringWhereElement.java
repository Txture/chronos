package org.chronos.chronodb.internal.impl.query.parser.ast;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.query.ContainmentCondition;
import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentStringSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

import java.util.Set;

import static com.google.common.base.Preconditions.*;

public class SetStringWhereElement extends WhereElement<Set<String>, StringContainmentCondition> {

    private final TextMatchMode matchMode;

    public SetStringWhereElement(final String indexName, StringContainmentCondition condition, TextMatchMode matchMode, Set<String> comparisonValues){
        super(indexName, condition, comparisonValues);
        checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
        this.matchMode = matchMode;
    }

    @Override
    public SetStringWhereElement negate() {
        return new SetStringWhereElement(this.indexName, this.condition.negate(), this.matchMode, this.comparisonValue);
    }

    @Override
    public SearchSpecification<?, ?> toSearchSpecification() {
        return ContainmentStringSearchSpecification.create(this.getIndexName(), this.getCondition(), this.matchMode, this.getComparisonValue());
    }


    public TextMatchMode getMatchMode() {
        return this.matchMode;
    }

    @Override
    public WhereElement<Set<String>, StringContainmentCondition> collapseToInClause(final WhereElement<?, ?> other) {
        if (!this.indexName.equals(other.getIndexName())) {
            // cannot mix queries on different properties (e.g. firstname and lastname)
            return null;
        }

        if (this.condition.equals(StringContainmentCondition.WITHIN)) {
            if (other instanceof StringWhereElement) {
                StringWhereElement otherStringWhere = (StringWhereElement) other;
                return otherStringWhere.collapseToInClause(this);
            } else if (other instanceof SetStringWhereElement) {
                SetStringWhereElement otherSetStringWhere = (SetStringWhereElement) other;
                if (otherSetStringWhere.getCondition().equals(StringContainmentCondition.WITHIN) && this.matchMode.equals(otherSetStringWhere.getMatchMode())) {
                    Set<String> inClause = Sets.newHashSet(otherSetStringWhere.getComparisonValue());
                    inClause.addAll(this.comparisonValue);
                    return new SetStringWhereElement(this.indexName, StringContainmentCondition.WITHIN, this.matchMode, inClause);
                }
            }
        } else if (this.condition.equals(StringContainmentCondition.WITHOUT)) {
            if (other instanceof StringWhereElement) {
                StringWhereElement otherLongWhere = (StringWhereElement) other;
                return otherLongWhere.collapseToInClause(this);
            } else if (other instanceof SetStringWhereElement) {
                SetStringWhereElement otherSetStringWhere = (SetStringWhereElement) other;
                if (otherSetStringWhere.getCondition().equals(StringContainmentCondition.WITHOUT) && this.matchMode.equals(otherSetStringWhere.getMatchMode())) {
                    Set<String> inClause = Sets.newHashSet(otherSetStringWhere.getComparisonValue());
                    inClause.addAll(this.comparisonValue);
                    return new SetStringWhereElement(this.indexName, StringContainmentCondition.WITHOUT, this.matchMode, inClause);
                }
            }
        }

        return null;
    }
}
