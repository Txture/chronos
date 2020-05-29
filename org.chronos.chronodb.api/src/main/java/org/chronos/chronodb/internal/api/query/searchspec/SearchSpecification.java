package org.chronos.chronodb.internal.api.query.searchspec;

import java.util.function.Predicate;

import org.chronos.chronodb.api.query.Condition;

public interface SearchSpecification<ELEMENTVALUE, SEARCHVALUE> {

	// =================================================================================================================
	// API
	// =================================================================================================================

	public String getProperty();

	public SEARCHVALUE getSearchValue();

	public Condition getCondition();

	public Predicate<Object> toFilterPredicate();

	public SearchSpecification<ELEMENTVALUE, SEARCHVALUE> negate();

	public boolean matches(final ELEMENTVALUE value);

	public String getDescriptiveSearchType();

}