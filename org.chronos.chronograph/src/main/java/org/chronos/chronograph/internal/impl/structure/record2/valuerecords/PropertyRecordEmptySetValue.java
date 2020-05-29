package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class PropertyRecordEmptySetValue implements PropertyRecordValue<Set<Object>> {

    public PropertyRecordEmptySetValue() {
        // default constructor for kryo
    }

    @Override
    public Set<Object> getValue() {
        return Collections.emptySet();
    }

    @Override
    public Set<Object> getSerializationSafeValue() {
        return Sets.newHashSet();
    }

    @Override
    public String toString() {
        return "[]";
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(final Object other) {
        if(other == null){
            return false;
        }
        return this.getClass().equals(other.getClass());
    }
}
