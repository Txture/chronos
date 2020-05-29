package org.chronos.chronodb.test.cases.util;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.indexing.StringIndexer;

import java.util.Set;

public class ReflectiveStringIndexer extends ReflectiveIndexer<String> implements StringIndexer {

    protected ReflectiveStringIndexer() {
        // default constructor for serialization
    }

    public ReflectiveStringIndexer(final Class<?> clazz, final String fieldName) {
        super(clazz, fieldName);
    }

    @Override
    public Set<String> getIndexValues(final Object object) {
        Object fieldValue = this.getFieldValue(object);
        Set<String> resultSet = Sets.newHashSet();
        if (fieldValue instanceof Iterable) {
            for (Object element : (Iterable<?>) fieldValue) {
                if (element instanceof String) {
                    resultSet.add((String) element);
                }
            }
        } else {
            if (fieldValue instanceof String) {
                resultSet.add((String) fieldValue);
            }
        }
        return resultSet;
    }

}