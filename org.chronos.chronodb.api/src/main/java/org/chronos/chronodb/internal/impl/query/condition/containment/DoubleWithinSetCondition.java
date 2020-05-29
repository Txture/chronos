package org.chronos.chronodb.internal.impl.query.condition.containment;

import org.chronos.chronodb.api.query.ContainmentCondition;
import org.chronos.chronodb.api.query.DoubleContainmentCondition;
import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import java.util.Set;

public class DoubleWithinSetCondition extends AbstractCondition implements DoubleContainmentCondition {

    public static final DoubleWithinSetCondition INSTANCE = new DoubleWithinSetCondition();

    protected DoubleWithinSetCondition() {
        super("double within");
    }

    @Override
    public DoubleContainmentCondition negate() {
        return SetWithoutDoubleCondition.INSTANCE;
    }

    @Override
    public boolean applies(final double elementToTest, final Set<Double> collectionToTestAgainst, final double equalityTolerance) {
        if(collectionToTestAgainst == null || collectionToTestAgainst.isEmpty()){
            return false;
        }
        double tolerance = Math.abs(equalityTolerance);
        for(Double e : collectionToTestAgainst){
            if(e == null){
                continue;
            }
            if(Math.abs(e - elementToTest) <= tolerance){
                return true;
            }
        }
        return false;
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
        return "Double Within";
    }
}
