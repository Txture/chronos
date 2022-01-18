package org.chronos.chronograph.internal.impl.optimizer.traversals

import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil
import org.apache.tinkerpop.gremlin.structure.Element

/**
 * The implementation of this class is equivalent to [ValueTraversal], except that it returns `null` when it
 * encounters a graph element that doesn't have the requested property (instead of throwing an exception).
 */
class ElementValueOrNullTraversal<V>(val propertyKey: String) : AbstractLambdaTraversal<Element?, V?>() {
    private var value: V? = null
    override fun next(): V? {
        return value
    }

    override fun hasNext(): Boolean {
        return true
    }

    override fun remove() {
        throw UnsupportedOperationException("remove() is not supported here")
    }

    @Suppress("UNCHECKED_CAST")
    override fun addStart(start: Traverser.Admin<Element?>) {
        if (null == bypassTraversal) {
            val o: Any? = start.get()
            value = when (o) {
                is Element -> o.property<Any>(propertyKey).orElse(null) as V?
                is Map<*, *> -> o[propertyKey] as V?
                else -> throw IllegalStateException(String.format(
                    "The by(\"%s\") modulator can only be applied to a traverser that is an Element or a Map - it is being applied to [%s] a %s class instead",
                    propertyKey, o, o!!.javaClass.simpleName))
            }
        } else {
            value = TraversalUtil.apply(start, bypassTraversal)
        }
    }

    override fun toString(): String {
        return "value(" + (if (null == bypassTraversal) propertyKey else bypassTraversal) + ')'
    }

    override fun hashCode(): Int {
        return super.hashCode() xor propertyKey.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return (other is ElementValueOrNullTraversal<*>
            && other.propertyKey == propertyKey)
    }
}