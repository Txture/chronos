package org.chronos.chronodb.internal.api.query.searchspec;

import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.NumberCondition;
import org.chronos.chronodb.internal.impl.query.LongSearchSpecificationImpl;

public interface LongSearchSpecification extends SearchSpecification<Long, Long> {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	public static LongSearchSpecification create(final SecondaryIndex index, final NumberCondition condition, final long searchValue) {
		return new LongSearchSpecificationImpl(index, condition, searchValue);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public NumberCondition getCondition();

	@Override
	public default boolean matches(final Long value) {
		return this.getCondition().applies(value, this.getSearchValue());
	}

	@Override
	public default String getDescriptiveSearchType() {
		return "Long";
	}

}
