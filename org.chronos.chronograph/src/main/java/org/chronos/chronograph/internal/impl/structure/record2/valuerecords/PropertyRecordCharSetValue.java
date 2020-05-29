package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import com.google.common.collect.Sets;
import org.chronos.common.annotation.PersistentClass;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordCharSetValue implements PropertyRecordValue<Set<Character>> {

    private Set<Character> values;

    protected PropertyRecordCharSetValue(){
        // default constructor for kryo
    }

    public PropertyRecordCharSetValue(Set<Character> values){
        checkNotNull(values, "Precondition violation - argument 'values' must not be NULL!");
        this.values = Sets.newHashSet(values);
    }

    @Override
    public Set<Character> getValue() {
        return Collections.unmodifiableSet(this.values);
    }

    @Override
    public Set<Character> getSerializationSafeValue() {
        return Sets.newHashSet(this.values);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordCharSetValue that = (PropertyRecordCharSetValue) o;

        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return values != null ? values.hashCode() : 0;
    }

    @Override
    public String toString() {
        return this.values.toString();
    }
}
