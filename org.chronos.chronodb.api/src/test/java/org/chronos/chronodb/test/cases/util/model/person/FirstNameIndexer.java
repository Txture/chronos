package org.chronos.chronodb.test.cases.util.model.person;

import org.chronos.common.test.utils.model.person.Person;

import java.util.Collections;
import java.util.Set;

public class FirstNameIndexer extends PersonIndexer {

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return super.equals(obj);
    }

    @Override
    protected Set<String> getIndexValuesInternal(final Person person) {
        return Collections.singleton(person.getFirstName());
    }

}
