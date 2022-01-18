package org.chronos.chronodb.internal.impl.query.parser.ast;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.*;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.StringSearchSpecification;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

import java.util.Objects;
import java.util.Set;

public class StringWhereElement extends WhereElement<String, StringCondition> {

    protected final TextMatchMode matchMode;

    public StringWhereElement(final String indexName, final StringCondition condition, final TextMatchMode matchMode, final String comparisonValue) {
        super(indexName, condition, comparisonValue);
        checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
        this.matchMode = matchMode;
    }

    public TextMatchMode getMatchMode() {
        return this.matchMode;
    }

    @Override
    public StringWhereElement negate() {
        return new StringWhereElement(this.getIndexName(), this.getCondition().negate(), this.getMatchMode(), this.getComparisonValue());
    }

    @Override
    public SearchSpecification<?, ?> toSearchSpecification(SecondaryIndex index) {
        if(!Objects.equals(index.getName(), this.indexName)){
            throw new IllegalArgumentException("Cannot use index '" + index.getName() + "' for search specification - expected index name is '" + this.indexName + "'!");
        }
        return StringSearchSpecification.create(index, this.getCondition(), this.getMatchMode(), this.getComparisonValue());
    }

    @Override
    public WhereElement<Set<String>, StringContainmentCondition> collapseToInClause(final WhereElement<?, ?> other) {
        if (!this.indexName.equals(other.getIndexName())) {
            // cannot mix queries on different properties (e.g. firstname and lastname)
            return null;
        }
        if (this.condition.equals(StringCondition.EQUALS)) {
            if (other instanceof StringWhereElement) {
                StringWhereElement otherStringWhere = (StringWhereElement) other;
                if (otherStringWhere.getCondition().equals(StringCondition.EQUALS) && this.matchMode.equals(otherStringWhere.getMatchMode())) {
                    return new SetStringWhereElement(this.indexName, StringContainmentCondition.WITHIN, this.matchMode, Sets.newHashSet(this.comparisonValue, otherStringWhere.getComparisonValue()));
                }
            } else if (other instanceof SetStringWhereElement) {
                SetStringWhereElement otherSetStringWhere = (SetStringWhereElement) other;
                if (otherSetStringWhere.getCondition().equals(StringContainmentCondition.WITHIN) && this.matchMode.equals(otherSetStringWhere.getMatchMode())) {
                    Set<String> inClause = Sets.newHashSet(otherSetStringWhere.getComparisonValue());
                    inClause.add(this.comparisonValue);
                    return new SetStringWhereElement(this.indexName, StringContainmentCondition.WITHIN, this.matchMode, inClause);
                }
            }
        } else if (this.condition.equals(StringCondition.NOT_EQUALS)) {
            if (other instanceof StringWhereElement) {
                StringWhereElement otherStringWhere = (StringWhereElement) other;
                if (otherStringWhere.getCondition().equals(StringCondition.NOT_EQUALS) && this.matchMode.equals(otherStringWhere.getMatchMode())) {
                    return new SetStringWhereElement(this.indexName, StringContainmentCondition.WITHOUT, this.matchMode, Sets.newHashSet(this.comparisonValue, otherStringWhere.getComparisonValue()));
                }
            } else if (other instanceof SetStringWhereElement) {
                SetStringWhereElement otherSetStringWhere = (SetStringWhereElement) other;
                if (otherSetStringWhere.getCondition().equals(StringContainmentCondition.WITHOUT) && this.matchMode.equals(otherSetStringWhere.getMatchMode())) {
                    Set<String> inClause = Sets.newHashSet(otherSetStringWhere.getComparisonValue());
                    inClause.add(this.comparisonValue);
                    return new SetStringWhereElement(this.indexName, StringContainmentCondition.WITHOUT, this.matchMode, inClause);
                }
            }
        }
        return null;
    }

}
