package org.chronos.chronograph.internal.impl.util;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.chronos.chronograph.api.structure.ChronoEdge;
import org.chronos.chronograph.api.structure.ChronoElement;
import org.chronos.chronograph.api.structure.ChronoVertex;
import org.chronos.chronograph.api.transaction.ChronoGraphTransaction;
import org.chronos.chronograph.internal.api.transaction.GraphTransactionContextInternal;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.AbstractElementProxy;

import java.util.Iterator;

import static com.google.common.base.Preconditions.*;

public class ChronoProxyUtil {

    @SuppressWarnings("unchecked")
    public static <T extends ChronoElement> T resolveProxy(final T maybeProxy) {
        if (maybeProxy == null) {
            return null;
        }
        if (maybeProxy instanceof AbstractElementProxy) {
            // proxy, resolve
            AbstractElementProxy<?> proxy = (AbstractElementProxy<?>) maybeProxy;
            return (T) proxy.getElement();
        } else {
            // not a proxy
            return maybeProxy;
        }
    }

    public static ChronoVertexImpl resolveVertexProxy(final Vertex vertex) {
        return (ChronoVertexImpl) resolveProxy((ChronoVertex) vertex);
    }

    public static ChronoEdgeImpl resolveEdgeProxy(final Edge edge) {
        return (ChronoEdgeImpl) resolveProxy((ChronoEdge) edge);
    }

    public static Iterator<Vertex> replaceVerticesByProxies(final Iterator<Vertex> iterator,
                                                            final ChronoGraphTransaction tx) {
        checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        GraphTransactionContextInternal context = (GraphTransactionContextInternal) tx.getContext();
        return Iterators.transform(iterator, v -> context.getOrCreateVertexProxy(v));
    }

    public static Iterator<Edge> replaceEdgesByProxies(final Iterator<Edge> iterator, final ChronoGraphTransaction tx) {
        checkNotNull(iterator, "Precondition violation - argument 'iterator' must not be NULL!");
        checkNotNull(tx, "Precondition violation - argument 'tx' must not be NULL!");
        GraphTransactionContextInternal context = (GraphTransactionContextInternal) tx.getContext();
        return Iterators.transform(iterator, e -> context.getOrCreateEdgeProxy(e));
    }

}
