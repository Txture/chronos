package org.chronos.chronograph.internal.impl.iterators.builder;

import java.util.Iterator;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.*;

public class SideEffectIterator<T> implements Iterator<T> {

    public static <T> Iterator<T> create(Iterator<T> iterator, BiConsumer<T, T> sideEffect) {
        return new SideEffectIterator<>(iterator, sideEffect);
    }


    private final Iterator<T> iterator;
    private final BiConsumer<T, T> sideEffect;

    private T current = null;

    public SideEffectIterator(Iterator<T> iterator, BiConsumer<T, T> sideEffect) {
        checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
        checkNotNull(sideEffect, "Precondition violation - argument 'sideEffect' must not be NULL!");
        this.iterator = iterator;
        this.sideEffect = sideEffect;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public T next() {
        T next = this.iterator.next();
        this.sideEffect.accept(this.current, next);
        this.current = next;
        return next;
    }
}
