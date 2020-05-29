package org.chronos.chronograph.test.util;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoEdgeImpl;
import org.chronos.chronograph.internal.impl.structure.graph.ChronoVertexImpl;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoEdgeProxy;
import org.chronos.chronograph.internal.impl.structure.graph.proxy.ChronoVertexProxy;

import static com.google.common.base.Preconditions.*;

public class ChronoGraphTestUtil {

    public static boolean isFullyLoaded(Element element) {
        checkNotNull(element, "Precondition violation - argument 'element' must not be NULL!");
        if (element instanceof ChronoVertexProxy) {
            ChronoVertexProxy vertexProxy = (ChronoVertexProxy) element;
            return vertexProxy.isLoaded();
        } else if (element instanceof ChronoVertexImpl) {
            return true;
        } else if (element instanceof ChronoEdgeProxy) {
            ChronoEdgeProxy edgeProxy = (ChronoEdgeProxy) element;
            return edgeProxy.isLoaded();
        } else if (element instanceof ChronoEdgeImpl) {
            return true;
        } else {
            throw new RuntimeException("Could not determine loaded state of element with class [" + element.getClass().getName() + "]");
        }

    }

}
