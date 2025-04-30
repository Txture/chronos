package org.chronos.chronograph.internal.impl.optimizer.step

import org.apache.tinkerpop.gremlin.structure.Element

interface Prefetching {

    fun registerFutureElementForPrefetching(element: Element)

}