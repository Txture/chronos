package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;
import org.chronos.common.serialization.KryoManager;

import java.util.Objects;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public class PropertyRecordCustomObjectValue implements PropertyRecordValue<Object> {

    protected byte[] value;

    protected PropertyRecordCustomObjectValue(){
        // default constructor for kryo
    }

    public PropertyRecordCustomObjectValue(Object value){
        checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
        this.value = KryoManager.serialize(value);
    }

    @Override
    public Object getValue() {
        return KryoManager.deserialize(this.value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PropertyRecordCustomObjectValue that = (PropertyRecordCustomObjectValue) o;

        return Objects.equals(this.getValue(), that.getValue());
    }

    @Override
    public int hashCode() {
        Object value = this.getValue();
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return String.valueOf(this.getValue());
    }
}
