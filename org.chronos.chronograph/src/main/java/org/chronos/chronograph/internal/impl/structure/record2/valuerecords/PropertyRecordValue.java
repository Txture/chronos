package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import org.chronos.common.annotation.PersistentClass;

import java.util.ArrayList;
import java.util.HashSet;

import static com.google.common.base.Preconditions.*;

@PersistentClass("kryo")
public interface PropertyRecordValue<T> {

    @SuppressWarnings("unchecked")
    public static <V> PropertyRecordValue<V> of(V value){
        checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
        return PropertyRecordValueUtil.createPropertyRecord(value);
    }

    public T getValue();

    /**
     * Returns a variant of this value which is safe for serialization purposes, but may come at additional runtime overhead.
     *
     * <p>
     * For example, collection-type property record values always return unmodifiable collections with {@link #getValue()}.
     * For this method, they return copies of the original collections which are modifiable (e.g. {@link ArrayList}, {@link HashSet} etc.).
     * </p>
     *
     * @return The serialization-safe value.
     */
    public default T getSerializationSafeValue(){
        // to be overridden in subclasses
        return this.getValue();
    }

}
