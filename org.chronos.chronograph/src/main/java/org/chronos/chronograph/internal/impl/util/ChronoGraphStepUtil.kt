package org.chronos.chronograph.internal.impl.util

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.api.index.ChronoGraphIndex
import org.chronos.chronograph.api.structure.ChronoGraph
import org.chronos.chronograph.internal.ChronoGraphConstants
import org.chronos.chronograph.internal.api.structure.ChronoGraphInternal

object ChronoGraphStepUtil {

    @JvmStatic
    fun getIndicesAndKeyspace(
        traversal: Traversal.Admin<*, *>,
        traversers: List<Traverser.Admin<Element>>,
        propertyKeys: Set<String>
    ): Pair<Set<ChronoGraphIndex>, String>? {
        val types = mutableSetOf<Class<out Any>>()
        val primaryKeys = HashSet<String>(traversers.size)
        for(traverser in traversers){
            val element = traverser.get()
            types += when (element) {
                is Vertex -> Vertex::class.java
                is Edge -> Edge::class.java
                else -> return null // unknown type, not indexable.
            }
            if(types.size > 1){
                // can't use index scan for mixed inputs
                return null
            }
            primaryKeys += element.id() as String
        }
        val graph = traversal.graph.orElse(null) as ChronoGraphInternal
        graph.tx().readWrite()
        val tx = ChronoGraphTraversalUtil.getTransaction(traversal)
        return if (Vertex::class.java in types) {
            // all elements are vertices. Are the properties indexed?
            if (this.areAllPropertiesIndexed(traversal, Vertex::class.java, propertyKeys)) {
                val vertexPropertyIndices = graph.getIndexManagerOnBranch(tx.branchName).getIndexedVertexPropertiesAtTimestamp(tx.timestamp)
                Pair(vertexPropertyIndices, ChronoGraphConstants.KEYSPACE_VERTEX)
            } else {
                // not all properties are indexed -> can't use index scan
                return null
            }
        } else if (Edge::class.java in types) {
            // all elements are edges. Are the properties indexed?
            if (this.areAllPropertiesIndexed(traversal, Edge::class.java, propertyKeys)) {
                val edgePropertyIndices = graph.getIndexManagerOnBranch(tx.branchName).getIndexedEdgePropertiesAtTimestamp(tx.timestamp)
                Pair(edgePropertyIndices, ChronoGraphConstants.KEYSPACE_EDGE)
            } else {
                // not all properties are indexed -> can't use index scan
                return null
            }
        } else {
            // we're dealing with some unknown input type -> can't use index scan
            return null
        }
    }

    private fun areAllPropertiesIndexed(traversal: Traversal.Admin<*, *>, type: Class<out Element>, propertyKeys: Set<String>): Boolean {
        val graph = traversal.graph.orElse(null) as? ChronoGraph?
            ?: return false

        graph.tx().readWrite()
        val tx = ChronoGraphTraversalUtil.getTransaction(traversal)

        val indexedPropertyNames = when (type) {
            Vertex::class.java -> graph.getIndexManagerOnBranch(tx.branchName).getIndexedVertexPropertyNamesAtTimestamp(tx.timestamp)
            Edge::class.java -> graph.getIndexManagerOnBranch(tx.branchName).getIndexedEdgePropertyNamesAtTimestamp(tx.timestamp)
            else -> emptySet()
        }
        for (propertyKey in propertyKeys) {
            if (propertyKey !in indexedPropertyNames) {
                return false
            }
        }
        return true
    }

}