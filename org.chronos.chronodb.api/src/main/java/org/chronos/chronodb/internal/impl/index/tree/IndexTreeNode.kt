package org.chronos.chronodb.internal.impl.index.tree

import org.chronos.chronodb.api.SecondaryIndex
import java.util.*

class IndexTreeNode(
    val index: SecondaryIndex,
    val parent: IndexTreeNode?
) {

    val children = mutableSetOf<IndexTreeNode>()

    init {
        parent?.children?.add(this)
    }

    inline fun <T> getDirectOrTransitiveChildren(map: (IndexTreeNode) -> T): Set<T> {
        val toVisit = Stack<IndexTreeNode>()
        val resultSet = mutableSetOf<T>()
        this.children.forEach(toVisit::push)
        while (toVisit.isNotEmpty()) {
            val current = toVisit.pop()
            resultSet.add(map(current))
            current.children.forEach(toVisit::push)
        }
        return resultSet
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as IndexTreeNode

        if (index != other.index) return false

        return true
    }

    override fun hashCode(): Int {
        return index.hashCode()
    }

    override fun toString(): String {
        return "IndexTreeNode($index)"
    }


}