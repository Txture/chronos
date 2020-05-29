package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

public class PropertyRecordEmptyListValue implements PropertyRecordValue<List<Object>> {

    public PropertyRecordEmptyListValue() {
        // default constructor for kryo
    }

    @Override
    public List<Object> getValue() {
        return Collections.emptyList();
    }

    @Override
    public List<Object> getSerializationSafeValue() {
        return Lists.newArrayList();
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
