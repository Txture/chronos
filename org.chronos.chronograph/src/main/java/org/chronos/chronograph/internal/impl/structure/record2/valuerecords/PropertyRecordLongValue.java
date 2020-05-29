package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class PropertyRecordLongValue implements PropertyRecordValue<Long> {

    private long value;

    protected PropertyRecordLongValue(){
        // default constructor for kryo
    }

    public PropertyRecordLongValue(long value){
        this.value = value;
    }

    @Override
    public Long getValue() {
        return this.value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordLongValue that = (PropertyRecordLongValue) o;

        return value == that.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}
