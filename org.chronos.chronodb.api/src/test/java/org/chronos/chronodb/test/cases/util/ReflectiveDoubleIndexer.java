package org.chronos.chronodb.test.cases.util;

import com.google.common.collect.Sets;
import org.chronos.chronodb.api.indexing.DoubleIndexer;
import org.chronos.common.util.ReflectionUtils;

import java.util.Set;

public class ReflectiveDoubleIndexer extends ReflectiveIndexer<Double> implements DoubleIndexer {

    public ReflectiveDoubleIndexer() {
        // default constructor for serialization
    }

    public ReflectiveDoubleIndexer(final Class<?> clazz, final String fieldName) {
        super(clazz, fieldName);
    }

    @Override
    public Set<Double> getIndexValues(final Object object) {
        Object fieldValue = this.getFieldValue(object);
        Set<Double> resultSet = Sets.newHashSet();
        if (fieldValue instanceof Iterable) {
            for (Object element : (Iterable<?>) fieldValue) {
                if (ReflectionUtils.isDoubleCompatible(element)) {
                    resultSet.add(ReflectionUtils.asDouble(element));
                }
            }
        } else {
            if (ReflectionUtils.isDoubleCompatible(fieldValue)) {
                resultSet.add(ReflectionUtils.asDouble(fieldValue));
            }
        }
        return resultSet;
    }

}