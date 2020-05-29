package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class PropertyRecordBooleanValue implements PropertyRecordValue<Boolean> {

    private boolean value;

    protected PropertyRecordBooleanValue(){
        // default constructor for kryo
    }

    public PropertyRecordBooleanValue(boolean value){
        this.value = value;
    }

    @Override
    public Boolean getValue() {
        return this.value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordBooleanValue that = (PropertyRecordBooleanValue) o;

        return value == that.value;
    }

    @Override
    public int hashCode() {
        return (value ? 1 : 0);
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}
