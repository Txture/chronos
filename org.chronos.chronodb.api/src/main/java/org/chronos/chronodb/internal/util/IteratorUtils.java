package org.chronos.chronodb.internal.util;

import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.*;

public class IteratorUtils {

    private IteratorUtils() {
        throw new UnsupportedOperationException("Do not instantiate this class!");
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    public static <T> Iterator<T> skipLast(final Iterator<T> iterator) {
        checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
        return new SkipLastIterator<>(iterator);
    }

    public static <T> Iterator<T> unique(final Iterator<T> iterator) {
        checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
        return new UniqueIterator<>(iterator);
    }

    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator,
            Spliterator.IMMUTABLE | Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private static class SkipLastIterator<T> implements Iterator<T> {

        private Iterator<T> innerIterator;

        private T nextElement;

        public SkipLastIterator(final Iterator<T> iterator) {
            checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
            this.innerIterator = iterator;
            this.tryFetchNext();
        }

        private void tryFetchNext() {
            this.nextElement = null;
            if (this.innerIterator.hasNext()) {
                T element = this.innerIterator.next();
                this.nextElement = element;
                if (this.innerIterator.hasNext() == false) {
                    // we found the last element, mark iterator as exhausted
                    this.nextElement = null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return this.nextElement != null;
        }

        @Override
        public T next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException("Iterator has no more elements!");
            }
            T element = this.nextElement;
            this.tryFetchNext();
            return element;
        }

    }

    private static class UniqueIterator<T> implements Iterator<T> {

        private Iterator<T> innerIterator;
        private Set<T> visited;

        private T nextElement;

        public UniqueIterator(final Iterator<T> iterator) {
            checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
            this.innerIterator = iterator;
            this.visited = Sets.newHashSet();
            this.tryFetchNext();
        }

        private void tryFetchNext() {
            this.nextElement = null;
            while (this.innerIterator.hasNext()) {
                T element = this.innerIterator.next();
                if (this.visited.contains(element)) {
                    // we have already seen this element; continue with the next one
                    continue;
                }
                // this is a new (unseen) element. Register it
                this.visited.add(element);
                this.nextElement = element;
                break;
            }
        }

        @Override
        public boolean hasNext() {
            return this.nextElement != null;
        }

        @Override
        public T next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException("Iterator has no more elements!");
            }
            T current = this.nextElement;
            this.tryFetchNext();
            return current;
        }
    }
}
