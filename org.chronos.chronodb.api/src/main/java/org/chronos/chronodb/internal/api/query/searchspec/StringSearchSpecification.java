package org.chronos.chronodb.internal.api.query.searchspec;

import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.StringCondition;
import org.chronos.chronodb.internal.impl.query.StringSearchSpecificationImpl;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;

public interface StringSearchSpecification extends SearchSpecification<String, String> {

	// =================================================================================================================
	// FACTORY METHODS
	// =================================================================================================================

	public static StringSearchSpecification create(final SecondaryIndex index, final StringCondition condition, final TextMatchMode matchMode, final String searchText) {
		return new StringSearchSpecificationImpl(index, condition, searchText, matchMode);
	}

	// =================================================================================================================
	// PUBLIC API
	// =================================================================================================================

	@Override
	public StringCondition getCondition();

	public TextMatchMode getMatchMode();

	@Override
	public default boolean matches(final String value) {
		return this.getCondition().applies(value, getSearchValue(), getMatchMode());
	}

	@Override
	public default String getDescriptiveSearchType() {
		return "String";
	}

}
