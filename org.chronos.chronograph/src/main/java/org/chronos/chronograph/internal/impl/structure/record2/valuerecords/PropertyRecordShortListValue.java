package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import com.google.common.collect.Lists;
import org.chronos.common.annotation.PersistentClass;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordShortListValue implements PropertyRecordValue<List<Short>> {

    private List<Short> values;

    protected PropertyRecordShortListValue(){
        // default constructor for kryo
    }

    public PropertyRecordShortListValue(List<Short> values){
        checkNotNull(values, "Precondition violation - argument 'values' must not be NULL!");
        this.values = Lists.newArrayList(values);
    }

    @Override
    public List<Short> getValue() {
        return Collections.unmodifiableList(this.values);
    }

    @Override
    public List<Short> getSerializationSafeValue() {
        return Lists.newArrayList(this.values);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordShortListValue that = (PropertyRecordShortListValue) o;

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
