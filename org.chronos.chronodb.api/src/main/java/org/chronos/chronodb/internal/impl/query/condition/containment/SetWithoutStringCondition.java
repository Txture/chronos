package org.chronos.chronodb.internal.impl.query.condition.containment;

import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;

import java.util.Set;

public class SetWithoutStringCondition extends AbstractCondition implements StringContainmentCondition {

    public static final SetWithoutStringCondition INSTANCE = new SetWithoutStringCondition();

    protected SetWithoutStringCondition() {
        super("string without");
    }

    @Override
    public StringWithinSetCondition negate() {
        return StringWithinSetCondition.INSTANCE;
    }

    @Override
    public boolean applies(final String elementToTest, final Set<String> collectionToTestAgainst, TextMatchMode matchMode) {
        if(collectionToTestAgainst == null || collectionToTestAgainst.isEmpty()){
            return true;
        }
        return !StringWithinSetCondition.INSTANCE.applies(elementToTest, collectionToTestAgainst, matchMode);
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
        return "String Without";
    }
}
