package org.chronos.chronodb.internal.impl.query;

import static com.google.common.base.Preconditions.*;

import com.google.common.primitives.Primitives;
import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.exceptions.InvalidIndexAccessException;
import org.chronos.chronodb.api.query.Condition;
import org.chronos.chronodb.internal.api.query.searchspec.SearchSpecification;

public abstract class AbstractSearchSpecification<ELEMENTVALUE, CONDITIONTYPE extends Condition, SEARCHVALUE> implements SearchSpecification<ELEMENTVALUE, SEARCHVALUE> {

	protected final SecondaryIndex index;
	protected final CONDITIONTYPE condition;
	protected final SEARCHVALUE searchValue;

	protected AbstractSearchSpecification(final SecondaryIndex index, final CONDITIONTYPE condition, final SEARCHVALUE searchValue) {
		checkNotNull(index, "Precondition violation - argument 'index' must not be NULL!");
		checkNotNull(condition, "Precondition violation - argument 'condition' must not be NULL!");
		checkNotNull(searchValue, "Precondition violation - argument 'searchValue' must not be NULL!");
		this.checkIndexType(index);
		this.index = index;
		this.condition = condition;
		this.searchValue = searchValue;
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public SecondaryIndex getIndex(){
		return this.index;
	}

	@Override
	public SEARCHVALUE getSearchValue() {
		return this.searchValue;
	}

	@Override
	public CONDITIONTYPE getCondition() {
		return this.condition;
	}

	// =================================================================================================================
	// HASH CODE & EQUALS
	// =================================================================================================================

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.condition == null ? 0 : this.condition.hashCode());
		result = prime * result + (this.index == null ? 0 : this.index.hashCode());
		result = prime * result + (this.searchValue == null ? 0 : this.searchValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		AbstractSearchSpecification<?, ?, ?> other = (AbstractSearchSpecification<?, ?, ?>) obj;
		if (this.condition == null) {
			if (other.condition != null) {
				return false;
			}
		} else if (!this.condition.equals(other.condition)) {
			return false;
		}
		if (this.index == null) {
			if (other.index != null) {
				return false;
			}
		} else if (!this.index.equals(other.index)) {
			return false;
		}
		if (this.searchValue == null) {
			if (other.searchValue != null) {
				return false;
			}
		} else if (!this.searchValue.equals(other.searchValue)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return this.getIndex() + " " + this.getCondition().getInfix() + " '" + this.getSearchValue() + "'";
	}

	protected abstract Class<ELEMENTVALUE> getElementValueClass();

	private void checkIndexType(SecondaryIndex index){
		Class<ELEMENTVALUE> expectedValueType = Primitives.wrap(this.getElementValueClass());
		Class<?> indexValueType = Primitives.wrap(index.getValueType());

		if (!expectedValueType.equals(indexValueType)) {
			throw new InvalidIndexAccessException(
				"Cannot create search specification of type '"
					+ expectedValueType.getSimpleName() +
					"' on index of type '"
					+ indexValueType.getSimpleName()
					+ "'!"
			);
		}
	}

}
