package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import java.util.Date;

public class PropertyRecordDateValue implements PropertyRecordValue<Date> {

    private long timestamp;

    protected PropertyRecordDateValue(){
        // default constructor for kryo
    }

    public PropertyRecordDateValue(Date date){
        this.timestamp = date.getTime();
    }

    @Override
    public Date getValue() {
        return new Date(this.timestamp);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordDateValue that = (PropertyRecordDateValue) o;

        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return (int) (timestamp ^ (timestamp >>> 32));
    }

    @Override
    public String toString() {
        return this.getValue().toString();
    }
}
