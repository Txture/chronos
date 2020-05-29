package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

import java.util.Arrays;
import java.util.Date;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordDateArrayValue implements PropertyRecordValue<Date[]> {

    private Long[] array;

    protected PropertyRecordDateArrayValue(){
        // default constructor for kryo
    }

    public PropertyRecordDateArrayValue(Date[] array){
        checkNotNull(array, "Precondition violation - argument 'array' must not be NULL!");
        this.array = new Long[array.length];
        for(int i = 0; i < this.array.length; i++){
            Date date = array[i];
            if(date == null){
                this.array[i] = null;
            }else{
                this.array[i] = date.getTime();
            }
        }
    }

    @Override
    public Date[] getValue() {
        Date[] outArray = new Date[this.array.length];
        for(int i = 0; i < this.array.length; i++){
            Long time = this.array[i];
            if(time == null){
                outArray[i] = null;
            }else{
                outArray[i] = new Date(time);
            }
        }
        return outArray;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordDateArrayValue that = (PropertyRecordDateArrayValue) o;

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
