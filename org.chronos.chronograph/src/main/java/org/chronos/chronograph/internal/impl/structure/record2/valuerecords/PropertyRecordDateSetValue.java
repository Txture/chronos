package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import com.google.common.collect.Sets;
import org.chronos.common.annotation.PersistentClass;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordDateSetValue implements PropertyRecordValue<Set<Date>> {

    private Set<Long> values;

    protected PropertyRecordDateSetValue(){
        // default constructor for kryo
    }

    public PropertyRecordDateSetValue(Set<Date> values){
        checkNotNull(values, "Precondition violation - argument 'values' must not be NULL!");
        this.values = Sets.newHashSetWithExpectedSize(values.size());
        for (Date date : values) {
            if (date == null) {
                this.values.add(null);
            } else {
                this.values.add(date.getTime());
            }
        }
    }

    @Override
    public Set<Date> getValue() {
        return Collections.unmodifiableSet(getDates());
    }

    @Override
    public Set<Date> getSerializationSafeValue() {
        return getDates();
    }

    @NotNull
    private Set<Date> getDates() {
        Set<Date> resultList = Sets.newHashSetWithExpectedSize(this.values.size());
        for (Long time : this.values) {
            if (time == null) {
                resultList.add(null);
            } else {
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

        PropertyRecordDateSetValue that = (PropertyRecordDateSetValue) o;

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
