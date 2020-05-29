package org.chronos.chronograph.test.cases.record;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.chronos.chronograph.internal.impl.structure.record2.valuerecords.*;
import org.chronos.common.test.ChronosUnitTest;
import org.junit.Test;

import java.util.Date;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class PropertyRecordValueTest extends ChronosUnitTest {

    @Test
    public void canCreatePropertyRecordBooleanValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordBooleanValue.class;
        assertPropertyRecordValueIsCorrect(clazz, true, false, Boolean.TRUE, Boolean.FALSE);
    }

    @Test
    public void canCreatePropertyRecordByteValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordByteValue.class;
        Byte byteVal1 = (byte) 12;
        Byte byteVal2 = (byte) -3;
        assertPropertyRecordValueIsCorrect(clazz, (byte) 12, (byte) -3, byteVal1, byteVal2);
    }

    @Test
    public void canCreatePropertyRecordShortValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordShortValue.class;
        Short shortVal1 = (short) 12;
        Short shortVal2 = (short) -3;
        assertPropertyRecordValueIsCorrect(clazz, (short) 12, (short) -3, shortVal1, shortVal2);
    }

    @Test
    public void canCreatePropertyRecordCharValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCharValue.class;
        Character charVal1 = 'a';
        Character charVal2 = '\u20ac';
        assertPropertyRecordValueIsCorrect(clazz, 'a', '\u20ac', charVal1, charVal2);
    }

    @Test
    public void canCreatePropertyRecordIntegerValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordIntValue.class;
        Integer intVal1 = 123;
        Integer intVal2 = -344;
        assertPropertyRecordValueIsCorrect(clazz, 123, -344, intVal1, intVal2);
    }

    @Test
    public void canCreatePropertyRecordLongValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordLongValue.class;
        Long longVal1 = 123L;
        Long longVal2 = -344L;
        assertPropertyRecordValueIsCorrect(clazz, 123L, -344L, longVal1, longVal2);
    }

    @Test
    public void canCreatePropertyRecordFloatValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordFloatValue.class;
        Float floatVal1 = 123.0f;
        Float floatVal2 = -344.0f;
        assertPropertyRecordValueIsCorrect(clazz, 123.0f, -344.0f, floatVal1, floatVal2);
    }

    @Test
    public void canCreatePropertyRecordDoubleValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDoubleValue.class;
        Double doubleVal1 = 123.0d;
        Double doubleVal2 = -344.0d;
        assertPropertyRecordValueIsCorrect(clazz, 123.0d, -344.0d, doubleVal1, doubleVal2);
    }

    @Test
    public void canCreatePropertyRecordStringValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordStringValue.class;
        assertPropertyRecordValueIsCorrect(clazz, "Lorem", "Hello", "World");
    }

    @Test
    public void canCreatePropertyRecordDateValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDateValue.class;
        assertPropertyRecordValueIsCorrect(clazz, new Date());
    }

    @Test
    public void canCreatePropertyRecordBooleanArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordBooleanArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new boolean[]{true, true, false});
    }

    @Test
    public void canCreatePropertyRecordBooleanWrapperArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordBooleanWrapperArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Boolean[]{Boolean.TRUE, Boolean.FALSE});
    }

    @Test
    public void canCreatePropertyRecordBooleanListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordBooleanListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList(Boolean.TRUE, null, false));
    }

    @Test
    public void canCreatePropertyRecordBooleanSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordBooleanSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet(Boolean.TRUE, null, false));
    }

    @Test
    public void canCreatePropertyRecordByteArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordByteArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new byte[]{12, -3});
    }

    @Test
    public void canCreatePropertyRecordByteWrapperArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordByteWrapperArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Byte[]{12, -3, null});
    }

    @Test
    public void canCreatePropertyRecordByteListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordByteListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList((byte)12, (byte)-3, null));
    }

    @Test
    public void canCreatePropertyRecordByteSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordByteSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet((byte)12, (byte)-3, null));
    }

    @Test
    public void canCreatePropertyRecordCharArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCharArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new char[]{'a', '@'});
    }

    @Test
    public void canCreatePropertyRecordCharWrapperArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCharWrapperArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Character[]{'a', null, '@'});
    }

    @Test
    public void canCreatePropertyRecordCharListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCharListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList('a', null, '@'));
    }

    @Test
    public void canCreatePropertyRecordCharSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCharSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet('a', null, '@'));
    }

    @Test
    public void canCreatePropertyRecordShortArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordShortArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new short[]{12, -3});
    }

    @Test
    public void canCreatePropertyRecordShortWrapperArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordShortWrapperArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Short[]{(short)12, (short)-3, null});
    }

    @Test
    public void canCreatePropertyRecordShortListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordShortListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList((short)12, (short)-3, null));
    }

    @Test
    public void canCreatePropertyRecordShortSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordShortSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet((short)12, (short)-3, null));
    }

    @Test
    public void canCreatePropertyRecordIntArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordIntArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new int[]{12, -3});
    }

    @Test
    public void canCreatePropertyRecordIntWrapperArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordIntWrapperArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Integer[]{12, null, -3});
    }

    @Test
    public void canCreatePropertyRecordIntListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordIntListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList(12, null, -3));
    }

    @Test
    public void canCreatePropertyRecordIntSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordIntSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet(12, null, -3));
    }

    @Test
    public void canCreatePropertyRecordLongArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordLongArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new long[]{12L, -3L});
    }

    @Test
    public void canCreatePropertyRecordLongWrapperArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordLongWrapperArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Long[]{12L, null, -3L});
    }

    @Test
    public void canCreatePropertyRecordLongListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordLongListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList(12L, null, -3L));
    }

    @Test
    public void canCreatePropertyRecordLongSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordLongSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet(12L, null, -3L));
    }

    @Test
    public void canCreatePropertyRecordFloatArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordFloatArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new float[]{12.8f, -3f});
    }

    @Test
    public void canCreatePropertyRecordFloatWrapperArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordFloatWrapperArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Float[]{12.8f, null, -3f});
    }

    @Test
    public void canCreatePropertyRecordFloatListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordFloatListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList(12.8f, null, -3f));
    }

    @Test
    public void canCreatePropertyRecordFloatSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordFloatSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet(12.8f, null, -3f));
    }

    @Test
    public void canCreatePropertyRecordDoubleArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDoubleArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new double[]{12.8d, -3d});
    }

    @Test
    public void canCreatePropertyRecordDoubleWrapperArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDoubleWrapperArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Double[]{12.8d, null, -3d});
    }

    @Test
    public void canCreatePropertyRecordDoubleListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDoubleListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList(12.8d, null, -3d));
    }

    @Test
    public void canCreatePropertyRecordDoubleSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDoubleSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet(12.8d, null, -3d));
    }

    @Test
    public void canCreatePropertyRecordStringArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordStringArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new String[]{"Lorem", "Ipsum", null});
    }

    @Test
    public void canCreatePropertyRecordStringListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordStringListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList("Lorem", "Ipsum", null));
    }

    @Test
    public void canCreatePropertyRecordStringSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordStringSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet("Lorem", "Ipsum", null));
    }

    @Test
    public void canCreatePropertyRecordDateArrayValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDateArrayValue.class;
        assertPropertyRecordValueIsCorrect(clazz, (Object) new Date[]{new Date(), null});
    }

    @Test
    public void canCreatePropertyRecordDateListValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDateListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList(new Date(), null));
    }

    @Test
    public void canCreatePropertyRecordDateSetValues() {
        Class<? extends PropertyRecordValue> clazz = PropertyRecordDateSetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet(new Date(), null));
    }

    @Test
    public void canCreatePropertyRecordForCustomValue(){
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCustomObjectValue.class;
        assertPropertyRecordValueIsCorrect(clazz, new MyClass("Hello!"));
    }

    @Test
    public void canCreatePropertyRecordForCustomListValue(){
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCustomObjectValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList(new MyClass("Hello!")));
    }

    @Test
    public void canCreatePropertyRecordForCustomSetValue(){
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCustomObjectValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet(new MyClass("Hello!")));
    }

    @Test
    public void canCreatePropertyRecordForListOfNullValues(){
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCustomObjectValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList(null, null));
    }

    @Test
    public void canCreatePropertyRecordForSetOfNullValues(){
        Class<? extends PropertyRecordValue> clazz = PropertyRecordCustomObjectValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet(null, null));
    }


    @Test
    public void canCreatePropertyRecordForEmptyList(){
        Class<? extends PropertyRecordValue> clazz = PropertyRecordEmptyListValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Lists.newArrayList());
    }

    @Test
    public void canCreatePropertyRecordForEmptySet(){
        Class<? extends PropertyRecordValue> clazz = PropertyRecordEmptySetValue.class;
        assertPropertyRecordValueIsCorrect(clazz, Sets.newHashSet());
    }


    // =================================================================================================================
    // INTERNAL HELPER METHODS
    // =================================================================================================================

    private void assertPropertyRecordValueIsCorrect(Class<?> valueWrapperClass, Object... testValues) {
        for (Object testValue : testValues) {
            PropertyRecordValue<Object> wrapper = PropertyRecordValue.of(testValue);
            assertThat(wrapper, is(instanceOf(valueWrapperClass)));
            assertThat(wrapper.getValue(), is(testValue));
        }
    }

    private static class MyClass {

        private String name;

        public MyClass(String name){
            this.name = name;
        }

        protected MyClass(){

        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            MyClass myClass = (MyClass) o;

            return name != null ? name.equals(myClass.name) : myClass.name == null;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }


}
