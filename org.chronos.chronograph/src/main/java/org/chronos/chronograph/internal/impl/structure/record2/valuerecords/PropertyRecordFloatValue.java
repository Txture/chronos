package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class PropertyRecordFloatValue implements PropertyRecordValue<Float> {

    private float value;

    protected PropertyRecordFloatValue(){
        // default constructor for kryo
    }

    public PropertyRecordFloatValue(float value){
        this.value = value;
    }

    @Override
    public Float getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordFloatValue that = (PropertyRecordFloatValue) o;

        return Float.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return (value != +0.0f ? Float.floatToIntBits(value) : 0);
    }
}
