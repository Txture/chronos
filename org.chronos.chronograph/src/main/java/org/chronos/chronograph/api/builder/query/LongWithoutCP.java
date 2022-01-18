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
    public int hashCode(){
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(Object other){
        if(other == null){
            return false;
        }
        return other instanceof LongWithoutCP;
    }

    @Override
    public String toString() {
        return "Long Without";
    }
}
