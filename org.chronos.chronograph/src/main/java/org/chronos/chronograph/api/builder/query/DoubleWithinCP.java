package org.chronos.chronograph.api.builder.query;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.BiPredicate;

public class DoubleWithinCP implements BiPredicate<Object, Collection> {

    private final double tolerance;

    public DoubleWithinCP(final double tolerance) {
        this.tolerance = tolerance;
    }

    public double getTolerance() {
        return tolerance;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean test(final Object o, final Collection collection) {
        if(collection == null || collection.isEmpty()){
            return false;
        }
        if(o instanceof Double){
            double value = (Double)o;
            for(Object element : collection){
                if(element instanceof Double){
                    double other = (Double)element;
                    if(Math.abs(value - other) <= this.tolerance){
                        return true;
                    }
                }
            }
        } else if(o instanceof Collection){
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
        return new DoubleWithoutCP(tolerance);
    }

    @Override
    public String toString() {
        return "Double Within";
    }
}
