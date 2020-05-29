package org.chronos.chronograph.api.structure.record

import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronograph.internal.impl.structure.record.*
import org.chronos.chronograph.internal.impl.structure.record2.EdgeRecord2
import org.chronos.chronograph.internal.impl.structure.record2.PropertyRecord2

class IEdgeRecordBuilder {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private var id: String? = null
    private var label: String? = null
    private var inVId: String? = null
    private var outVId: String? = null
    private val properties: MutableSet<PropertyRecord2> = mutableSetOf()

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    fun fromEdge(edge: Edge): IEdgeRecordBuilder {
        this.withId(edge.id() as String)
        withLabel(edge.label())
        withInVertex(edge.inVertex().id() as String)
        withOutVertex(edge.outVertex().id() as String)
        edge.properties<Any>().forEach{ this.withProperty(it) }
        return this
    }

    fun fromRecord(edgeRecord: IEdgeRecord): IEdgeRecordBuilder {
        this.withId(edgeRecord.id)
        withLabel(edgeRecord.label)
        withInVertex(edgeRecord.inVertexId)
        withOutVertex(edgeRecord.outVertexId)
        edgeRecord.properties.forEach{ this.withProperty(it) }
        return this
    }

    fun withId(id: String): IEdgeRecordBuilder {
        this.id = id
        return this
    }

    fun withLabel(label: String): IEdgeRecordBuilder {
        this.label = label
        return this
    }

    fun withInVertex(vertex: Vertex): IEdgeRecordBuilder {
        return this.withInVertex(vertex.id() as String)
    }

    fun withInVertex(vertexRecord: IVertexRecord): IEdgeRecordBuilder {
        return this.withInVertex(vertexRecord.id)
    }

    fun withInVertex(vertexId: String): IEdgeRecordBuilder {
        this.inVId = vertexId
        return this
    }

    fun withOutVertex(vertex: Vertex): IEdgeRecordBuilder {
        return this.withOutVertex(vertex.id() as String)
    }

    fun withOutVertex(vertexRecord: IVertexRecord): IEdgeRecordBuilder {
        return this.withOutVertex(vertexRecord.id)
    }

    fun withOutVertex(vertexId: String): IEdgeRecordBuilder {
        this.outVId = vertexId
        return this
    }

    fun withProperty(propertyName: String, propertyValue: Any): IEdgeRecordBuilder {
        this.properties.removeIf { it.key == propertyName }
        this.properties += PropertyRecord2(propertyName, propertyValue)
        return this
    }

    fun withProperty(property: Property<*>): IEdgeRecordBuilder {
        return this.withProperty(property.key(), property.value())
    }

    fun withProperty(propertyRecord: IPropertyRecord): IEdgeRecordBuilder {
        this.properties += PropertyRecord2(propertyRecord.key, propertyRecord.value)
        return this
    }

    fun withoutProperty(propertyName: String): IEdgeRecordBuilder {
        this.properties.removeIf{ it.key == propertyName }
        return this
    }

    fun withoutProperties(): IEdgeRecordBuilder {
        this.properties.clear()
        return this
    }

    fun build(): IEdgeRecord {
        if(this.id == null){
            throw IllegalStateException("Cannot create IEdgeRecord without an ID!")
        }
        if(this.label == null){
            throw IllegalStateException("Cannot create IEdgeRecord without a label!")
        }
        if(this.inVId == null){
            throw IllegalStateException("Cannot create IEdgeRecord without an In-Vertex ID!")
        }
        if(this.outVId == null){
            throw IllegalStateException("Cannot create IEdgeRecord without an Out-Vertex ID!")
        }
        return EdgeRecord2(this.id, this.outVId, this.label, this.inVId, this.properties)
    }

}