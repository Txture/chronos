package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class PropertyRecordShortValue implements PropertyRecordValue<Short> {

    private short value;

    protected PropertyRecordShortValue(){
        // default constructor for kryo
    }

    public PropertyRecordShortValue(short value){
        this.value = value;
    }

    @Override
    public Short getValue() {
        return this.value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordShortValue that = (PropertyRecordShortValue) o;

        return value == that.value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}
