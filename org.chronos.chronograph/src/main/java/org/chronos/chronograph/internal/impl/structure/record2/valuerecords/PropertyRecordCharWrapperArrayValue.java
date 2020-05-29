package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

import java.util.Arrays;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordCharWrapperArrayValue implements PropertyRecordValue<Character[]> {

    private Character[] array;

    protected PropertyRecordCharWrapperArrayValue(){
        // default constructor for kryo
    }

    public PropertyRecordCharWrapperArrayValue(Character[] array){
        checkNotNull(array, "Precondition violation - argument 'array' must not be NULL!");
        this.array = new Character[array.length];
        System.arraycopy(array, 0, this.array, 0, array.length);
    }

    @Override
    public Character[] getValue() {
        Character[] outArray = new Character[this.array.length];
        System.arraycopy(this.array, 0, outArray, 0, array.length);
        return outArray;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordCharWrapperArrayValue that = (PropertyRecordCharWrapperArrayValue) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
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
