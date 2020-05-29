package org.chronos.chronodb.internal.impl.query.condition.containment;

import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;

import java.util.Set;

public class SetWithoutDoubleCondition extends AbstractCondition implements DoubleContainmentCondition {

    public static final SetWithoutDoubleCondition INSTANCE = new SetWithoutDoubleCondition();

    protected SetWithoutDoubleCondition() {
        super("double without");
    }

    @Override
    public DoubleWithinSetCondition negate() {
        return DoubleWithinSetCondition.INSTANCE;
    }

    @Override
    public boolean applies(final double elementToTest, final Set<Double> collectionToTestAgainst, double equalityTolerance) {
        if(collectionToTestAgainst == null || collectionToTestAgainst.isEmpty()){
            return true;
        }
        return !DoubleWithinSetCondition.INSTANCE.applies(elementToTest, collectionToTestAgainst, equalityTolerance);
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
        return "Double Without";
    }
}
