package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

import java.util.Arrays;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordIntWrapperArrayValue implements PropertyRecordValue<Integer[]> {

    private Integer[] array;

    protected PropertyRecordIntWrapperArrayValue(){
        // default constructor for kryo
    }

    public PropertyRecordIntWrapperArrayValue(Integer[] array){
        checkNotNull(array, "Precondition violation - argument 'array' must not be NULL!");
        this.array = new Integer[array.length];
        System.arraycopy(array, 0, this.array, 0, array.length);
    }

    @Override
    public Integer[] getValue() {
        Integer[] outArray = new Integer[this.array.length];
        System.arraycopy(this.array, 0, outArray, 0, array.length);
        return outArray;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordIntWrapperArrayValue that = (PropertyRecordIntWrapperArrayValue) o;

        return Arrays.equals(array, that.array);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);
    }

    @Override
    public String toString() {
        return Arrays.toString(this.array);
    }
    
}
