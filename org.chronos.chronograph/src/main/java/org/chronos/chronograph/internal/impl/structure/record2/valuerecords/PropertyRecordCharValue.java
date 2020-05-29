package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

@PersistentClass("kryo")
public class PropertyRecordCharValue implements PropertyRecordValue<Character> {

    private char value;

    protected PropertyRecordCharValue(){
        // default constructor for kryo
    }

    public PropertyRecordCharValue(char value){
        this.value = value;
    }

    @Override
    public Character getValue() {
        return this.value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordCharValue that = (PropertyRecordCharValue) o;

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
