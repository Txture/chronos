package org.chronos.chronodb.internal.impl.query.parser.ast;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.ContainmentCondition;
import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.api.query.searchspec.ContainmentDoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Objects;
import java.util.Set;

public class SetDoubleWhereElement extends WhereElement<Set<Double>, DoubleContainmentCondition> {

    private final double equalityTolerance;

    public SetDoubleWhereElement(final String indexName, DoubleContainmentCondition condition, Set<Double> comparisonValues, double equalityTolerance) {
        super(indexName, condition, comparisonValues);
        this.equalityTolerance = equalityTolerance;
    }

    @Override
    public SetDoubleWhereElement negate() {
        return new SetDoubleWhereElement(this.indexName, this.condition.negate(), this.comparisonValue, this.equalityTolerance);
    }

    @Override
    public SearchSpecification<?, ?> toSearchSpecification(SecondaryIndex index) {
        if(!Objects.equals(index.getName(), this.indexName)){
            throw new IllegalArgumentException("Cannot use index '" + index.getName() + "' for search specification - expected index name is '" + this.indexName + "'!");
        }
        return ContainmentDoubleSearchSpecification.create(index, this.getCondition(), this.getComparisonValue(), this.equalityTolerance);
    }


    public double getEqualityTolerance() {
        return equalityTolerance;
    }

    @Override
    public WhereElement<Set<Double>, DoubleContainmentCondition> collapseToInClause(final WhereElement<?, ?> other) {
        if (!this.indexName.equals(other.getIndexName())) {
            // cannot mix queries on different properties (e.g. firstname and lastname)
            return null;
        }

        if (this.condition.equals(DoubleContainmentCondition.WITHIN)) {
            if (other instanceof DoubleWhereElement) {
                DoubleWhereElement otherDoubleWhere = (DoubleWhereElement) other;
                return otherDoubleWhere.collapseToInClause(this);
            } else if (other instanceof SetDoubleWhereElement) {
                SetDoubleWhereElement otherSetDoubleWhere = (SetDoubleWhereElement) other;
                if (otherSetDoubleWhere.getCondition().equals(DoubleContainmentCondition.WITHIN) && this.equalityTolerance == otherSetDoubleWhere.getEqualityTolerance()) {
                    Set<Double> inClause = Sets.newHashSet(otherSetDoubleWhere.getComparisonValue());
                    inClause.addAll(this.comparisonValue);
                    return new SetDoubleWhereElement(this.indexName, DoubleContainmentCondition.WITHIN, inClause, this.equalityTolerance);
                }
            }
        } else if (this.condition.equals(DoubleContainmentCondition.WITHOUT)) {
            if (other instanceof DoubleWhereElement) {
                DoubleWhereElement otherLongWhere = (DoubleWhereElement) other;
                return otherLongWhere.collapseToInClause(this);
            } else if (other instanceof SetDoubleWhereElement) {
                SetDoubleWhereElement otherSetDoubleWhere = (SetDoubleWhereElement) other;
                if (otherSetDoubleWhere.getCondition().equals(DoubleContainmentCondition.WITHOUT) && this.equalityTolerance == otherSetDoubleWhere.getEqualityTolerance()) {
                    Set<Double> inClause = Sets.newHashSet(otherSetDoubleWhere.getComparisonValue());
                    inClause.addAll(this.comparisonValue);
                    return new SetDoubleWhereElement(this.indexName, DoubleContainmentCondition.WITHOUT, inClause, this.equalityTolerance);
                }
            }
        }

        return null;
    }
}
