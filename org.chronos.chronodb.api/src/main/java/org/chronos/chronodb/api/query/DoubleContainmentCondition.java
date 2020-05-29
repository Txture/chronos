package org.chronos.chronodb.api.query;

import org.chronos.chronodb.internal.impl.query.condition.containment.DoubleWithinSetCondition;
import org.chronos.chronodb.internal.impl.query.condition.containment.SetWithoutDoubleCondition;

import java.util.Set;

public interface DoubleContainmentCondition extends ContainmentCondition {

    public static DoubleContainmentCondition WITHIN = DoubleWithinSetCondition.INSTANCE;
    public static DoubleContainmentCondition WITHOUT = SetWithoutDoubleCondition.INSTANCE;

    /**
     * Negates this condition.
     *
     * @return The negated condition.
     */
    public DoubleContainmentCondition negate();

    /**
     * Applies this condition.
     *
     * @param elementToTest The element to check. May be <code>null</code>.
     * @param collectionToTestAgainst The collection to check against. <code>null</code> will be treated as empty collection.
     * @param tolerance The equality tolerance to apply when checking for containment.
     * @return <code>true</code> if this condition applies, otherwise <code>false</code>.
     */
    public boolean applies(double elementToTest, Set<Double> collectionToTestAgainst, double tolerance);

}
