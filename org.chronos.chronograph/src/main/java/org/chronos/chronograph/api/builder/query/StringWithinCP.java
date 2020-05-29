package org.chronos.chronograph.api.builder.query;

import org.chronos.chronodb.internal.impl.query.TextMatchMode;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.*;

public class StringWithinCP implements BiPredicate<Object, Collection> {

    private final TextMatchMode matchMode;

    public StringWithinCP(TextMatchMode matchMode) {
        checkNotNull(matchMode, "Precondition violation - argument 'matchMode' must not be NULL!");
        this.matchMode = matchMode;
    }

    public TextMatchMode getMatchMode() {
        return this.matchMode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean test(final Object o, final Collection collection) {
        if(collection == null || collection.isEmpty()){
            return false;
        }
        if(o instanceof String){
            switch(this.matchMode){
                case STRICT:
                    return collection.contains(o);
                case CASE_INSENSITIVE:
                    for(Object element : collection){
                        if(element instanceof String) {
                            if (((String) element).equalsIgnoreCase((String)o)) {
                                return true;
                            }
                        }
                    }
                    return false;
                default:
                    throw new UnknownEnumLiteralException(this.matchMode);
            }
        }else if(o instanceof Collection){
            for(Object element : (Collection<Object>)o){
                if(test(element, collection)){
                    return true;
                }
            }
        }
        return false;
    }

    @NotNull
    @Override
    public BiPredicate<Object, Collection> negate() {
        return new StringWithoutCP(this.matchMode);
    }

    @Override
    public String toString() {
        return "String Within" + (this.matchMode == TextMatchMode.CASE_INSENSITIVE ? " [CI]" : "");
    }


}
