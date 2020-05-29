package org.chronos.chronodb.api.query;

import org.chronos.chronodb.internal.impl.query.condition.containment.LongWithinSetCondition;
import org.chronos.chronodb.internal.impl.query.condition.containment.SetWithoutLongCondition;

import java.util.Set;

public interface LongContainmentCondition extends ContainmentCondition {

    public static LongContainmentCondition WITHIN = LongWithinSetCondition.INSTANCE;
    public static LongContainmentCondition WITHOUT = SetWithoutLongCondition.INSTANCE;


    /**
     * Negates this condition.
     *
     * @return The negated condition.
     */
    public LongContainmentCondition negate();

    /**
     * Applies this condition.
     *
     * @param elementToTest The element to check. May be <code>null</code>.
     * @param collectionToTestAgainst The collection to check against. <code>null</code> will be treated as empty collection.
     * @return <code>true</code> if this condition applies, otherwise <code>false</code>.
     */
    public boolean applies(long elementToTest, Set<Long> collectionToTestAgainst);

}
