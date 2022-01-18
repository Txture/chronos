package org.chronos.chronodb.internal.api.query.searchspec;

import java.util.function.Predicate;

import org.chronos.chronodb.api.SecondaryIndex;
import org.chronos.chronodb.api.query.Condition;

public interface SearchSpecification<ELEMENTVALUE, SEARCHVALUE> {

	// =================================================================================================================
	// API
	// =================================================================================================================

	public SecondaryIndex getIndex();

	public SEARCHVALUE getSearchValue();

	public Condition getCondition();

	public Predicate<Object> toFilterPredicate();

	public SearchSpecification<ELEMENTVALUE, SEARCHVALUE> onIndex(SecondaryIndex index);

	public SearchSpecification<ELEMENTVALUE, SEARCHVALUE> negate();

	public boolean matches(final ELEMENTVALUE value);

	public String getDescriptiveSearchType();

}