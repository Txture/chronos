package org.chronos.chronograph.internal.impl.query;

import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BiPredicate;

public class ChronoCompareUtil {


    public static boolean compare(Object left, Object right, BiPredicate<Object, Object> compareAtomic){
        Collection<Object> leftCollection = liftToCollection(left);
        Collection<Object> rightCollection = liftToCollection(right);
        if(leftCollection.isEmpty() || rightCollection.isEmpty()){
            // NULLs or empty collections never lead to any matches.
            return false;
        }
        if(leftCollection.size() == 1){
            Object onlyLeft = Iterables.getOnlyElement(leftCollection);
            if(rightCollection.size() == 1){
                // single-vs-single: compare directly
                Object onlyRight = Iterables.getOnlyElement(rightCollection);
                return compareAtomic.test(onlyLeft, onlyRight);
            }else{
                // single-vs-multi: never match
                return false;
            }
        }else {
            if(rightCollection.size() == 1){
                // multi-vs-single: evaluate all entries
                Object onlyRight = Iterables.getOnlyElement(rightCollection);
                for(Object entry : leftCollection){
                    if(compareAtomic.test(entry, onlyRight)){
                        return true;
                    }
                }
                return false;
            }else{
                // multi-vs-multi:
                return new MultiCompare(compareAtomic).test(leftCollection, rightCollection);
            }
        }
    }


    @SuppressWarnings("unchecked")
    public static Collection<Object> liftToCollection(Object value){
        if(value == null){
            return Collections.emptyList();
        }
        if(value instanceof Collection){
            return (Collection<Object>)value;
        }else{
            return Collections.singletonList(value);
        }
    }

    private static class MultiCompare implements BiPredicate<Collection<Object>, Collection<Object>> {

        private BiPredicate<Object, Object> compareAtomic;

        public MultiCompare(BiPredicate<Object, Object> compareAtomic){
            this.compareAtomic = compareAtomic;
        }

        @Override
        public boolean test(final Collection<Object> left, final Collection<Object> right) {
            for(Object leftElement : left){
                if(right.stream().noneMatch(rightElement -> this.compareAtomic.test(leftElement, rightElement))){
                    return false;
                }
            }
            return true;
        }
    }

}
