package org.chronos.chronodb.internal.impl.query.parser.ast;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.query.ContainmentCondition;
import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentLongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentStringSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Set;

public class SetLongWhereElement extends WhereElement<Set<Long>, LongContainmentCondition> {

    public SetLongWhereElement(final String indexName, LongContainmentCondition condition, Set<Long> comparisonValues){
        super(indexName, condition, comparisonValues);
    }

    @Override
    public SetLongWhereElement negate() {
        return new SetLongWhereElement(this.indexName, this.condition.negate(), this.comparisonValue);
    }

    @Override
    public SearchSpecification<?, ?> toSearchSpecification() {
        return ContainmentLongSearchSpecification.create(this.getIndexName(), this.getCondition(), this.getComparisonValue());
    }

    @Override
    public WhereElement<Set<Long>, LongContainmentCondition> collapseToInClause(final WhereElement<?, ?> other) {
        if (!this.indexName.equals(other.getIndexName())) {
            // cannot mix queries on different properties (e.g. firstname and lastname)
            return null;
        }

        if (this.condition.equals(LongContainmentCondition.WITHIN)) {
            if (other instanceof LongWhereElement) {
                LongWhereElement otherLongWhere = (LongWhereElement) other;
                return otherLongWhere.collapseToInClause(this);
            } else if (other instanceof SetLongWhereElement) {
                SetLongWhereElement otherSetLongWhere = (SetLongWhereElement) other;
                if (otherSetLongWhere.getCondition().equals(LongContainmentCondition.WITHIN)) {
                    Set<Long> inClause = Sets.newHashSet(otherSetLongWhere.getComparisonValue());
                    inClause.addAll(this.comparisonValue);
                    return new SetLongWhereElement(this.indexName, LongContainmentCondition.WITHIN, inClause);
                }
            }
        } else if (this.condition.equals(LongContainmentCondition.WITHOUT)) {
            if (other instanceof LongWhereElement) {
                LongWhereElement otherLongWhere = (LongWhereElement) other;
                return otherLongWhere.collapseToInClause(this);
            } else if (other instanceof SetLongWhereElement) {
                SetLongWhereElement otherSetLongWhere = (SetLongWhereElement) other;
                if (otherSetLongWhere.getCondition().equals(LongContainmentCondition.WITHOUT)) {
                    Set<Long> inClause = Sets.newHashSet(otherSetLongWhere.getComparisonValue());
                    inClause.addAll(this.comparisonValue);
                    return new SetLongWhereElement(this.indexName, LongContainmentCondition.WITHOUT, inClause);
                }
            }
        }

        return null;
    }
}
