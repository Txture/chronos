package org.chronos.chronograph.api.builder.query;

import org.chronos.chronograph.internal.impl.query.ChronoCompareUtil;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiPredicate;

public class DoubleEqualsCP implements BiPredicate<Object, Object> {

    private final double tolerance;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public DoubleEqualsCP(final double tolerance) {
        this.tolerance = tolerance;
    }

    public double getTolerance() {
        return this.tolerance;
    }

    @Override
    public boolean test(final Object o, final Object o2) {
        return ChronoCompareUtil.compare(o, o2, this::testAtomic);
    }

    private boolean testAtomic(final Object o, final Object o2){
        if(o instanceof Number == false || o2 instanceof Number == false){
            return false;
        }
        double left = ((Number)o).doubleValue();
        double right = ((Number)o2).doubleValue();
        return Math.abs(left - right) < this.tolerance;
    }


    @NotNull
    @Override
    public BiPredicate<Object, Object> negate() {
        return new DoubleNotEqualsCP(this.tolerance);
    }

    @Override
    public String toString() {
        return "Double Eq";
    }

}
