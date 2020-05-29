package org.chronos.chronodb.test.cases.util;

import org.chronos.chronodb.api.indexing.Indexer;
import org.chronos.common.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.Objects;

import static com.google.common.base.Preconditions.*;

public abstract class ReflectiveIndexer<T> implements Indexer<T> {

    private Class<?> clazz;
    private String fieldName;

    protected ReflectiveIndexer() {
        // default constructor for serialization
    }

    public ReflectiveIndexer(final Class<?> clazz, final String fieldName) {
        checkNotNull(clazz, "Precondition violation - argument 'clazz' must not be NULL!");
        checkNotNull(fieldName, "Precondition violation - argument 'fieldName' must not be NULL!");
        this.clazz = clazz;
        this.fieldName = fieldName;
    }

    @Override
    public boolean canIndex(final Object object) {
        if (object == null) {
            return false;
        }
        return this.clazz.isInstance(object);
    }

    protected Object getFieldValue(final Object object) {
        if (object == null) {
            return null;
        }
        try {
            Field field = ReflectionUtils.getDeclaredField(object.getClass(), this.fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new IllegalStateException("Failed to access field '" + this.fieldName + "' in instance of class '" + object.getClass().getName() + "'!", e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ReflectiveIndexer<?> that = (ReflectiveIndexer<?>) o;

        if (!Objects.equals(clazz, that.clazz))
            return false;
        return Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        int result = clazz != null ? clazz.hashCode() : 0;
        result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
        return result;
    }
}