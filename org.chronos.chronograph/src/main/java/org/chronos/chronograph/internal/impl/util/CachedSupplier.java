package org.chronos.chronograph.internal.impl.util;

import org.apache.tinkerpop.gremlin.structure.T;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class CachedSupplier<T> implements Supplier<T> {

    public static <T> CachedSupplier<T> create(Supplier<T> supplier){
        if(supplier instanceof CachedSupplier){
            return (CachedSupplier<T>)supplier;
        }else{
            return new CachedSupplier<>(supplier);
        }
    }


    private final Supplier<T> supplier;
    private T element;

    public CachedSupplier(Supplier<T> supplier){
        this.supplier = supplier;
    }

    public synchronized T get(){
        if(this.element == null){
            this.element = this.supplier.get();
        }
        return this.element;
    }

    public synchronized T getIfLoaded(){
        return this.element;
    }

    public synchronized <R> R mapIfLoaded(Function<T, R> function){
        if(this.element != null){
            return function.apply(this.element);
        }else{
            return null;
        }
    }

    public synchronized void doIfLoaded(Consumer<T> function){
        if(this.element != null){
            function.accept(this.element);
        }
    }

    public <R> CachedSupplier<R> map(Function<T, R> map){
        return new CachedSupplier<>(() -> map.apply(CachedSupplier.this.get()));
    }

}
