package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class PropertyRecordByteValue implements PropertyRecordValue<Byte> {

    private byte value;

    protected PropertyRecordByteValue(){
        // default constructor for kryo
    }

    public PropertyRecordByteValue(byte value){
        this.value = value;
    }

    @Override
    public Byte getValue() {
        return this.value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordByteValue that = (PropertyRecordByteValue) o;

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
