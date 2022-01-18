package org.chronos.chronodb.internal.impl.query.parser.ast;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.*;
import org.chronos.chronodb.internal.api.query.searchspec.DoubleSearchSpecification;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

import java.util.Objects;
import java.util.Set;

public class DoubleWhereElement extends WhereElement<Double, NumberCondition> {

	private final double equalityTolerance;

	public DoubleWhereElement(final String indexName, final NumberCondition condition, final double comparisonValue, final double equalityTolerance) {
		super(indexName, condition, comparisonValue);
		checkArgument(equalityTolerance >= 0, "Precondition violation - argument 'equalityTolerance' must not be negative!");
		this.equalityTolerance = equalityTolerance;
	}

	public double getEqualityTolerance() {
		return this.equalityTolerance;
	}

	@Override
	public DoubleWhereElement negate() {
		return new DoubleWhereElement(this.getIndexName(), this.getCondition().negate(), this.getComparisonValue(), this.getEqualityTolerance());
	}

	@Override
	public SearchSpecification<?, ?> toSearchSpecification(SecondaryIndex index) {
		if(!Objects.equals(index.getName(), this.indexName)){
			throw new IllegalArgumentException("Cannot use index '" + index.getName() + "' for search specification - expected index name is '" + this.indexName + "'!");
		}
		return DoubleSearchSpecification.create(index, this.getCondition(), this.getComparisonValue(), this.getEqualityTolerance());
	}

	@Override
	public WhereElement<Set<Double>, DoubleContainmentCondition> collapseToInClause(final WhereElement<?, ?> other) {
		if (!this.indexName.equals(other.getIndexName())) {
			// cannot mix queries on different properties (e.g. firstname and lastname)
			return null;
		}
		if (this.condition.equals(NumberCondition.EQUALS)) {
			if (other instanceof DoubleWhereElement) {
				DoubleWhereElement otherDoubleWhere = (DoubleWhereElement) other;
				if (otherDoubleWhere.getCondition().equals(NumberCondition.EQUALS) && this.equalityTolerance == otherDoubleWhere.getEqualityTolerance()) {
					return new SetDoubleWhereElement(this.indexName, DoubleContainmentCondition.WITHIN, Sets.newHashSet(this.comparisonValue, otherDoubleWhere.getComparisonValue()), this.equalityTolerance);
				}
			} else if (other instanceof SetDoubleWhereElement) {
				SetDoubleWhereElement otherSetStringWhere = (SetDoubleWhereElement) other;
				if (otherSetStringWhere.getCondition().equals(DoubleContainmentCondition.WITHIN) && this.equalityTolerance == otherSetStringWhere.getEqualityTolerance()) {
					Set<Double> inClause = Sets.newHashSet(otherSetStringWhere.getComparisonValue());
					inClause.add(this.comparisonValue);
					return new SetDoubleWhereElement(this.indexName, DoubleContainmentCondition.WITHIN, inClause, this.equalityTolerance);
				}
			}
		} else if (this.condition.equals(NumberCondition.NOT_EQUALS)) {
			if (other instanceof DoubleWhereElement) {
				DoubleWhereElement otherDoubleWhere = (DoubleWhereElement) other;
				if (otherDoubleWhere.getCondition().equals(NumberCondition.NOT_EQUALS) && this.equalityTolerance == otherDoubleWhere.getEqualityTolerance()) {
					return new SetDoubleWhereElement(this.indexName, DoubleContainmentCondition.WITHOUT, Sets.newHashSet(this.comparisonValue, otherDoubleWhere.getComparisonValue()), this.equalityTolerance);
				}
			} else if (other instanceof SetDoubleWhereElement) {
				SetDoubleWhereElement otherSetDoubleWhere = (SetDoubleWhereElement) other;
				if (otherSetDoubleWhere.getCondition().equals(DoubleContainmentCondition.WITHOUT) && this.equalityTolerance == otherSetDoubleWhere.getEqualityTolerance()) {
					Set<Double> inClause = Sets.newHashSet(otherSetDoubleWhere.getComparisonValue());
					inClause.add(this.comparisonValue);
					return new SetDoubleWhereElement(this.indexName, DoubleContainmentCondition.WITHOUT, inClause, this.equalityTolerance);
				}
			}
		}
		return null;
	}


}
