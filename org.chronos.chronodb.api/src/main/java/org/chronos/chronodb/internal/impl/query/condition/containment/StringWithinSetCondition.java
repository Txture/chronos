package org.chronos.chronodb.internal.impl.query.condition.containment;

import org.chronos.chronodb.api.query.StringContainmentCondition;
import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.chronodb.internal.impl.query.condition.AbstractCondition;
import org.chronos.common.exceptions.UnknownEnumLiteralException;

import java.util.Set;

public class StringWithinSetCondition extends AbstractCondition implements StringContainmentCondition {

    public static final StringWithinSetCondition INSTANCE = new StringWithinSetCondition();

    protected StringWithinSetCondition() {
        super("string within");
    }

    @Override
    public StringContainmentCondition negate() {
        return SetWithoutStringCondition.INSTANCE;
    }

    @Override
    public boolean applies(final String elementToTest, final Set<String> collectionToTestAgainst, final TextMatchMode matchMode) {
        if(collectionToTestAgainst == null || collectionToTestAgainst.isEmpty()){
            return false;
        }
        switch(matchMode){
            case STRICT: return collectionToTestAgainst.contains(elementToTest);
            case CASE_INSENSITIVE:
                for(String e : collectionToTestAgainst){
                    if(e == null){
                        if(elementToTest == null){
                            return true;
                        }else{
                            continue;
                        }
                    }
                    if(e.equalsIgnoreCase(elementToTest)){
                        return true;
                    }
                }
                return false;
            default:
                throw new UnknownEnumLiteralException(matchMode);
        }
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
        return "String Within";
    }
}
