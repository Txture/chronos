package org.chronos.chronograph.internal.impl.builder.graph;

import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.common.builder.ChronoBuilder;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphDefaultProperties {

    public static void applyTo(ChronoBuilder<?> builder){
        checkNotNull(builder, "Precondition violation - argument 'builder' must not be NULL!");
        // in ChronoGraph, we can ALWAYS ensure immutability of ChronoDB cache values. The reason for this is
        // that ChronoGraph only passes records (e.g. VertexRecord) to the underlying ChronoDB, and records
        // are always immutable.
        builder.withProperty(ChronoDBConfiguration.ASSUME_CACHE_VALUES_ARE_IMMUTABLE, "true");
    }
}
