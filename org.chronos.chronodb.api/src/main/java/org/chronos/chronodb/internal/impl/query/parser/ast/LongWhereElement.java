package org.chronos.chronodb.internal.impl.query.parser.ast;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.query.ContainmentCondition;
import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.api.query.searchspec.LongSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Set;

public class LongWhereElement extends WhereElement<Long, NumberCondition> {

    public LongWhereElement(final String indexName, final NumberCondition condition, final Long comparisonValue) {
        super(indexName, condition, comparisonValue);
    }

    @Override
    public LongWhereElement negate() {
        return new LongWhereElement(this.getIndexName(), this.getCondition().negate(), this.getComparisonValue());
    }

    @Override
    public SearchSpecification<?, ?> toSearchSpecification() {
        return LongSearchSpecification.create(this.getIndexName(), this.getCondition(), this.getComparisonValue());
    }

    @Override
    public WhereElement<Set<Long>, LongContainmentCondition> collapseToInClause(final WhereElement<?, ?> other) {
        if (!this.indexName.equals(other.getIndexName())) {
            // cannot mix queries on different properties (e.g. firstname and lastname)
            return null;
        }
        if (this.condition.equals(NumberCondition.EQUALS)) {
            if (other instanceof LongWhereElement) {
                LongWhereElement otherLongWhere = (LongWhereElement) other;
                if (otherLongWhere.getCondition().equals(NumberCondition.EQUALS)) {
                    return new SetLongWhereElement(this.indexName, LongContainmentCondition.WITHIN, Sets.newHashSet(this.comparisonValue, otherLongWhere.getComparisonValue()));
                }
            } else if (other instanceof SetLongWhereElement) {
                SetLongWhereElement otherSetLongWhere = (SetLongWhereElement) other;
                if (otherSetLongWhere.getCondition().equals(LongContainmentCondition.WITHIN)) {
                    Set<Long> inClause = Sets.newHashSet(otherSetLongWhere.getComparisonValue());
                    inClause.add(this.comparisonValue);
                    return new SetLongWhereElement(this.indexName, LongContainmentCondition.WITHIN, inClause);
                }
			}
        } else if (this.condition.equals(NumberCondition.NOT_EQUALS)) {
            if (other instanceof LongWhereElement) {
                LongWhereElement otherLongWhere = (LongWhereElement) other;
                if (otherLongWhere.getCondition().equals(NumberCondition.NOT_EQUALS)) {
                    return new SetLongWhereElement(this.indexName, LongContainmentCondition.WITHOUT, Sets.newHashSet(this.comparisonValue, otherLongWhere.getComparisonValue()));
                }
            } else if (other instanceof SetLongWhereElement) {
                SetLongWhereElement otherSetLongWhere = (SetLongWhereElement) other;
                if (otherSetLongWhere.getCondition().equals(LongContainmentCondition.WITHOUT)) {
                    Set<Long> inClause = Sets.newHashSet(otherSetLongWhere.getComparisonValue());
                    inClause.add(this.comparisonValue);
                    return new SetLongWhereElement(this.indexName, LongContainmentCondition.WITHOUT, inClause);
                }
			}
        }
        // in any other case, we cannot collapse
        return null;
    }
}
