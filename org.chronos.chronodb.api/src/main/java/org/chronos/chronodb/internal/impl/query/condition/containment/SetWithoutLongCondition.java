package org.chronos.chronodb.internal.impl.query.condition.containment;

import org.chronos.chronodb.api.query.ContainmentCondition;
import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.api.query.LongContainmentCondition;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;

import java.util.Set;

public class SetWithoutLongCondition extends AbstractCondition implements LongContainmentCondition {

    public static final SetWithoutLongCondition INSTANCE = new SetWithoutLongCondition();

    protected SetWithoutLongCondition() {
        super("long without");
    }

    @Override
    public LongWithinSetCondition negate() {
        return LongWithinSetCondition.INSTANCE;
    }

    @Override
    public boolean applies(final long elementToTest, final Set<Long> collectionToTestAgainst) {
        if(collectionToTestAgainst == null || collectionToTestAgainst.isEmpty()){
            return true;
        }
        return !LongWithinSetCondition.INSTANCE.applies(elementToTest, collectionToTestAgainst);
    }

    @Override
    public boolean isNegated() {
        return true;
    }

    @Override
    public boolean acceptsEmptyValue() {
        return true;
    }

    @Override
    public String toString() {
        return "Long Without";
    }
}
