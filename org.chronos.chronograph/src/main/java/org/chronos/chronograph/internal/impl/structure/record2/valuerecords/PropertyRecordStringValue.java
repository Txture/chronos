package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordStringValue implements PropertyRecordValue<String> {

    private String value;

    protected PropertyRecordStringValue(){
        // default constructor for kryo
    }

    public PropertyRecordStringValue(String value) {
        checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordStringValue that = (PropertyRecordStringValue) o;

        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }
}
