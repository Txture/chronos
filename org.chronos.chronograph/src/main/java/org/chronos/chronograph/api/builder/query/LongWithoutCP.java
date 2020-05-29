package org.chronos.chronograph.api.builder.query;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.BiPredicate;

public class LongWithoutCP implements BiPredicate<Object, Collection> {

    public LongWithoutCP() {
    }

    @Override
    public boolean test(final Object o, final Collection collection) {
        return !this.negate().test(o, collection);
    }

    @NotNull
    @Override
    public BiPredicate<Object, Collection> negate() {
        return new LongWithinCP();
    }

    @Override
    public String toString() {
        return "Long Without";
    }
}
