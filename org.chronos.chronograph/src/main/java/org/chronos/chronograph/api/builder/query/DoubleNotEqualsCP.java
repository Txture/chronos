package org.chronos.chronograph.api.builder.query;

import org.jetbrains.annotations.NotNull;

import java.util.function.BiPredicate;

public class DoubleNotEqualsCP implements BiPredicate<Object, Object> {

    private final double tolerance;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public DoubleNotEqualsCP(final double tolerance) {
        this.tolerance = tolerance;
    }

    public double getTolerance() {
        return this.tolerance;
    }

    @Override
    public boolean test(final Object o, final Object o2) {
        return !this.negate().test(o, o2);
    }


    @NotNull
    @Override
    public BiPredicate<Object, Object> negate() {
        return new DoubleEqualsCP(this.tolerance);
    }

    @Override
    public String toString() {
        return "Double Neq";
    }

}
