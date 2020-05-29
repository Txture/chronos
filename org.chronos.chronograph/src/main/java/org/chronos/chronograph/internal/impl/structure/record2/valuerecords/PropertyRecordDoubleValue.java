package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class PropertyRecordDoubleValue implements PropertyRecordValue<Double> {

    private double value;

    protected PropertyRecordDoubleValue(){
        // default constructor for kryo
    }

    public PropertyRecordDoubleValue(double value){
        this.value = value;
    }

    @Override
    public Double getValue() {
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

        PropertyRecordDoubleValue that = (PropertyRecordDoubleValue) o;

        return Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(value);
        return (int) (temp ^ (temp >>> 32));
    }
}
