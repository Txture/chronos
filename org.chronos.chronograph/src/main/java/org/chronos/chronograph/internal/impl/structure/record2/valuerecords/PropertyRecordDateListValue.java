package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import com.google.common.collect.Lists;
import org.chronos.common.annotation.PersistentClass;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordDateListValue implements PropertyRecordValue<List<Date>> {

    private List<Long> values;

    protected PropertyRecordDateListValue(){
        // default constructor for kryo
    }

    public PropertyRecordDateListValue(List<Date> values){
        checkNotNull(values, "Precondition violation - argument 'values' must not be NULL!");
        this.values = Lists.newArrayListWithExpectedSize(values.size());
        for (Date date : values) {
            if (date == null) {
                this.values.add(null);
            } else {
                this.values.add(date.getTime());
            }
        }
    }

    @Override
    public List<Date> getValue() {
        return Collections.unmodifiableList(getDates());
    }

    @Override
    public List<Date> getSerializationSafeValue() {
        return this.getDates();
    }


    private List<Date> getDates() {
        List<Date> resultList = Lists.newArrayListWithExpectedSize(this.values.size());
        for(Long time : this.values){
            if(time == null){
                resultList.add(null);
            }else{
                resultList.add(new Date(time));
            }
        }
        return resultList;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordDateListValue that = (PropertyRecordDateListValue) o;

        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return values != null ? values.hashCode() : 0;
    }

    @Override
    public String toString() {
        return this.values.toString();
    }
}
