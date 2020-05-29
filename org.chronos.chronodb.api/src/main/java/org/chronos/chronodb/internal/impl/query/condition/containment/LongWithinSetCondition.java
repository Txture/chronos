package org.chronos.chronodb.internal.impl.query.condition.containment;

import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;

import java.util.Set;

public class LongWithinSetCondition extends AbstractCondition implements LongContainmentCondition {

    public static final LongWithinSetCondition INSTANCE = new LongWithinSetCondition();

    protected LongWithinSetCondition() {
        super("long within");
    }

    @Override
    public LongContainmentCondition negate() {
        return SetWithoutLongCondition.INSTANCE;
    }

    @Override
    public boolean applies(final long elementToTest, final Set<Long> collectionToTestAgainst) {
        if(collectionToTestAgainst == null || collectionToTestAgainst.isEmpty()){
            return false;
        }
        return collectionToTestAgainst.contains(elementToTest);
    }

    @Override
    public boolean isNegated() {
        return false;
    }

    @Override
    public boolean acceptsEmptyValue() {
        return false;
    }

    @Override
    public String toString() {
        return "Long Within";
    }
}
