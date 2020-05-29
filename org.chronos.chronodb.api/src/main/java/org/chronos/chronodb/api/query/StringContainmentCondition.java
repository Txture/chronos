package org.chronos.chronodb.api.query;

import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.containment.SetWithoutStringCondition;
import org.chronos.chronodb.internal.impl.query.condition.containment.StringWithinSetCondition;

import java.util.Set;

public interface StringContainmentCondition extends ContainmentCondition {

    public static StringContainmentCondition WITHIN = StringWithinSetCondition.INSTANCE;
    public static StringContainmentCondition WITHOUT = SetWithoutStringCondition.INSTANCE;

    /**
     * Negates this condition.
     *
     * @return The negated condition.
     */
    public StringContainmentCondition negate();

    /**
     * Applies this condition.
     *
     * @param elementToTest The element to check. May be <code>null</code>.
     * @param collectionToTestAgainst The collection to check against. <code>null</code> will be treated as empty collection.
     * @param matchMode The text match mode to use when testing containment. Must not be <code>null</code>.
     * @return <code>true</code> if this condition applies, otherwise <code>false</code>.
     */
    public boolean applies(String elementToTest, Set<String> collectionToTestAgainst, TextMatchMode matchMode);

}
