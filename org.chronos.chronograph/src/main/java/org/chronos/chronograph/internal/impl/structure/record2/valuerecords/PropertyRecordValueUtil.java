package org.chronos.chronograph.internal.impl.structure.record2.valuerecords;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.function.Function;

import static com.google.common.base.Preconditions.*;

public class PropertyRecordValueUtil {

    private static final Map<Class<?>, Function<Object, PropertyRecordValue<?>>> PROPERTY_VALUE_CREATORS;

    private static final Map<Class<?>, Function<Object, PropertyRecordValue<? extends List<?>>>> PROPERTY_LIST_VALUE_CREATORS;
    private static final Map<Class<?>, Function<Object, PropertyRecordValue<? extends Set<?>>>> PROPERTY_SET_VALUE_CREATORS;

    static {
        Map<Class<?>, Function<Object, PropertyRecordValue<?>>> classToValueCreator = Maps.newHashMap();
        Map<Class<?>, Function<Object, PropertyRecordValue<? extends List<?>>>> classToListValueCreator = Maps.newHashMap();
        Map<Class<?>, Function<Object, PropertyRecordValue<? extends Set<?>>>> classToSetValueCreator = Maps.newHashMap();

        // primitives & primitve wrappers
        classToValueCreator.put(boolean.class, (Object value) -> new PropertyRecordBooleanValue((boolean)value));
        classToValueCreator.put(Boolean.class, (Object value) -> new PropertyRecordBooleanValue((boolean)value));
        classToValueCreator.put(boolean[].class, (Object value) -> new PropertyRecordBooleanArrayValue((boolean[])value));
        classToValueCreator.put(Boolean[].class, (Object value) -> new PropertyRecordBooleanWrapperArrayValue((Boolean[])value));
        classToListValueCreator.put(Boolean.class, (Object value) -> new PropertyRecordBooleanListValue((List<Boolean>)value));
        classToSetValueCreator.put(Boolean.class, (Object value) -> new PropertyRecordBooleanSetValue((Set<Boolean>)value));

        classToValueCreator.put(byte.class, (Object value) -> new PropertyRecordByteValue((byte)value));
        classToValueCreator.put(Byte.class, (Object value) -> new PropertyRecordByteValue((byte)value));
        classToValueCreator.put(byte[].class, (Object value) -> new PropertyRecordByteArrayValue((byte[])value));
        classToValueCreator.put(Byte[].class, (Object value) -> new PropertyRecordByteWrapperArrayValue((Byte[])value));
        classToListValueCreator.put(Byte.class, (Object value) -> new PropertyRecordByteListValue((List<Byte>)value));
        classToSetValueCreator.put(Byte.class, (Object value) -> new PropertyRecordByteSetValue((Set<Byte>)value));

        classToValueCreator.put(short.class, (Object value) -> new PropertyRecordShortValue((short)value));
        classToValueCreator.put(Short.class, (Object value) -> new PropertyRecordShortValue((short)value));
        classToValueCreator.put(short[].class, (Object value) -> new PropertyRecordShortArrayValue((short[])value));
        classToValueCreator.put(Short[].class, (Object value) -> new PropertyRecordShortWrapperArrayValue((Short[])value));
        classToListValueCreator.put(Short.class, (Object value) -> new PropertyRecordShortListValue((List<Short>)value));
        classToSetValueCreator.put(Short.class, (Object value) -> new PropertyRecordShortSetValue((Set<Short>)value));

        classToValueCreator.put(char.class, (Object value) -> new PropertyRecordCharValue((char)value));
        classToValueCreator.put(Character.class, (Object value) -> new PropertyRecordCharValue((char)value));
        classToValueCreator.put(char[].class, (Object value) -> new PropertyRecordCharArrayValue((char[])value));
        classToValueCreator.put(Character[].class, (Object value) -> new PropertyRecordCharWrapperArrayValue((Character[])value));
        classToListValueCreator.put(Character.class, (Object value) -> new PropertyRecordCharListValue((List<Character>)value));
        classToSetValueCreator.put(Character.class, (Object value) -> new PropertyRecordCharSetValue((Set<Character>)value));

        classToValueCreator.put(int.class, (Object value) -> new PropertyRecordIntValue((int)value));
        classToValueCreator.put(Integer.class, (Object value) -> new PropertyRecordIntValue((int)value));
        classToValueCreator.put(int[].class, (Object value) -> new PropertyRecordIntArrayValue((int[])value));
        classToValueCreator.put(Integer[].class, (Object value) -> new PropertyRecordIntWrapperArrayValue((Integer[])value));
        classToListValueCreator.put(Integer.class, (Object value) -> new PropertyRecordIntListValue((List<Integer>)value));
        classToSetValueCreator.put(Integer.class, (Object value) -> new PropertyRecordIntSetValue((Set<Integer>)value));

        classToValueCreator.put(long.class, (Object value) -> new PropertyRecordLongValue((long)value));
        classToValueCreator.put(Long.class, (Object value) -> new PropertyRecordLongValue((long)value));
        classToValueCreator.put(long[].class, (Object value) -> new PropertyRecordLongArrayValue((long[])value));
        classToValueCreator.put(Long[].class, (Object value) -> new PropertyRecordLongWrapperArrayValue((Long[])value));
        classToListValueCreator.put(Long.class, (Object value) -> new PropertyRecordLongListValue((List<Long>)value));
        classToSetValueCreator.put(Long.class, (Object value) -> new PropertyRecordLongSetValue((Set<Long>)value));

        classToValueCreator.put(float.class, (Object value) -> new PropertyRecordFloatValue((float)value));
        classToValueCreator.put(Float.class, (Object value) -> new PropertyRecordFloatValue((float)value));
        classToValueCreator.put(float[].class, (Object value) -> new PropertyRecordFloatArrayValue((float[])value));
        classToValueCreator.put(Float[].class, (Object value) -> new PropertyRecordFloatWrapperArrayValue((Float[])value));
        classToListValueCreator.put(Float.class, (Object value) -> new PropertyRecordFloatListValue((List<Float>)value));
        classToSetValueCreator.put(Float.class, (Object value) -> new PropertyRecordFloatSetValue((Set<Float>)value));

        classToValueCreator.put(double.class, (Object value) -> new PropertyRecordDoubleValue((double)value));
        classToValueCreator.put(Double.class, (Object value) -> new PropertyRecordDoubleValue((double)value));
        classToValueCreator.put(double[].class, (Object value) -> new PropertyRecordDoubleArrayValue((double[])value));
        classToValueCreator.put(Double[].class, (Object value) -> new PropertyRecordDoubleWrapperArrayValue((Double[])value));
        classToListValueCreator.put(Double.class, (Object value) -> new PropertyRecordDoubleListValue((List<Double>)value));
        classToSetValueCreator.put(Double.class, (Object value) -> new PropertyRecordDoubleSetValue((Set<Double>)value));

        // special support, "built-in" classes
        classToValueCreator.put(String.class, (Object value) -> new PropertyRecordStringValue((String)value));
        classToValueCreator.put(String[].class, (Object value) -> new PropertyRecordStringArrayValue((String[])value));
        classToListValueCreator.put(String.class, (Object value) -> new PropertyRecordStringListValue((List<String>)value));
        classToSetValueCreator.put(String.class, (Object value) -> new PropertyRecordStringSetValue((Set<String>)value));

        classToValueCreator.put(Date.class, (Object value) -> new PropertyRecordDateValue((Date)value));
        classToValueCreator.put(Date[].class, (Object value) -> new PropertyRecordDateArrayValue((Date[])value));
        classToListValueCreator.put(Date.class, (Object value) -> new PropertyRecordDateListValue((List<Date>)value));
        classToSetValueCreator.put(Date.class, (Object value) -> new PropertyRecordDateSetValue((Set<Date>)value));


        // collection support
        PROPERTY_VALUE_CREATORS = Collections.unmodifiableMap(classToValueCreator);
        PROPERTY_LIST_VALUE_CREATORS = Collections.unmodifiableMap(classToListValueCreator);
        PROPERTY_SET_VALUE_CREATORS = Collections.unmodifiableMap(classToSetValueCreator);
    }

    @SuppressWarnings("unchecked")
    public static <T> PropertyRecordValue<T> createPropertyRecord(T value){
        checkNotNull(value, "Precondition violation - argument 'value' must not be NULL!");
        if(value instanceof List && value.getClass().getName().startsWith("java.util")){
            if(((List<?>)value).isEmpty()){
                return (PropertyRecordValue<T>) new PropertyRecordEmptyListValue();
            }
            return createListValue((List)value);
        }else if(value instanceof Set && value.getClass().getName().startsWith("java.util")){
            if(((Set<?>)value).isEmpty()){
                return (PropertyRecordValue<T>) new PropertyRecordEmptySetValue();
            }
            return createSetValue((Set)value);
        } else{
            Function<Object, PropertyRecordValue<?>> creator = PROPERTY_VALUE_CREATORS.get(value.getClass());
            if(creator == null){
                // fall back to generic object
                creator = PropertyRecordCustomObjectValue::new;
            }
            return (PropertyRecordValue<T>) creator.apply(value);
        }

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> PropertyRecordValue<List<T>> createListValue(List<T> values) {
        Class<?> elementClass = null;
        for(T value : values){
            if(value == null) {
                continue;
            }
            if(elementClass == null){
                elementClass = value.getClass();
            }else if(!elementClass.equals(value.getClass())){
                // heterogeneous classes in list -> treat as generic object, but duplicate the list to eliminate unmodifiablelists.
                PropertyRecordValue result = new PropertyRecordCustomObjectValue(Lists.newArrayList(values));
                return (PropertyRecordValue<List<T>>) result;
            }
        }
        if(elementClass == null){
            // we have a list of only NULLs... fall back to generic object
            PropertyRecordValue result = new PropertyRecordCustomObjectValue(values);
            return (PropertyRecordValue<List<T>>) result;
        }
        Function<Object, PropertyRecordValue<? extends List<?>>> creator = PROPERTY_LIST_VALUE_CREATORS.get(elementClass);
        if(creator == null){
            // custom element class -> fall back to generic object, but duplicate the list to eliminate unmodifiablelists.
            PropertyRecordValue result = new PropertyRecordCustomObjectValue(Lists.newArrayList(values));
            return (PropertyRecordValue<List<T>>) result;
        }else{
            return (PropertyRecordValue<List<T>>) creator.apply(values);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> PropertyRecordValue<Set<T>> createSetValue(Set<T> values){
        Class<?> elementClass = null;
        for(T value : values){
            if(value == null) {
                continue;
            }
            if(elementClass == null){
                elementClass = value.getClass();
            }else if(!elementClass.equals(value.getClass())){
                // heterogeneous classes in list -> treat as generic object
                PropertyRecordValue result = new PropertyRecordCustomObjectValue(Sets.newHashSet(values));
                return (PropertyRecordValue<Set<T>>) result;
            }
        }
        if(elementClass == null){
            // we have a list of only NULLs... fall back to generic object
            PropertyRecordValue result = new PropertyRecordCustomObjectValue(values);
            return (PropertyRecordValue<Set<T>>) result;
        }
        Function<Object, PropertyRecordValue<? extends Set<?>>> creator = PROPERTY_SET_VALUE_CREATORS.get(elementClass);
        if(creator == null){
            // custom element class -> fall back to generic object
            PropertyRecordValue result = new PropertyRecordCustomObjectValue(Sets.newHashSet(values));
            return (PropertyRecordValue<Set<T>>) result;
        }else{
            return (PropertyRecordValue<Set<T>>) creator.apply(values);
        }
    }

}
