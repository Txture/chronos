package org.chronos.chronodb.internal.api.query.searchspec;

import org.chronos.chronodb.api.query.ContainmentCondition;

import java.util.Set;

public interface ContainmentSearchSpecification<T> extends SearchSpecification<T, Set<T>> {

    @Override
    public ContainmentCondition getCondition();

}
