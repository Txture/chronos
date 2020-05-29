package org.chronos.chronograph.api.structure.record

import com.google.common.collect.HashMultimap
import com.google.common.collect.SetMultimap
import org.apache.tinkerpop.gremlin.structure.*
import org.chronos.chronograph.internal.impl.structure.record.*
import org.chronos.chronograph.internal.impl.structure.record2.EdgeTargetRecord2
import org.chronos.chronograph.internal.impl.structure.record2.PropertyRecord2
import org.chronos.chronograph.internal.impl.structure.record3.SimpleVertexPropertyRecord
import org.chronos.chronograph.internal.impl.structure.record3.VertexPropertyRecord3
import org.chronos.chronograph.internal.impl.structure.record3.VertexRecord3

class IVertexRecordBuilder {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private var id: String? = null
    private var label: String? = null
    private val properties: MutableMap<String, VProp> = mutableMapOf()
    private val outEdgeTargetRecords: SetMultimap<String, EdgeTargetRecord2> = HashMultimap.create()
    private val inEdgeTargetRecords: SetMultimap<String, EdgeTargetRecord2> = HashMultimap.create()

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    fun fromVertex(vertex: Vertex): IVertexRecordBuilder {
        this.withId(vertex.id() as String)
        this.withLabel(vertex.label())
        vertex.properties<Any>().forEachRemaining { this.withProperty(it) }
        vertex.edges(Direction.OUT).forEachRemaining { this.withEdge(Direction.OUT, it.label(), it.id() as String, it.inVertex().id() as String) }
        vertex.edges(Direction.IN).forEachRemaining { this.withEdge(Direction.IN, it.label(), it.id() as String, it.outVertex().id() as String) }
        return this
    }

    fun fromRecord(record: IVertexRecord): IVertexRecordBuilder {
        this.withId(record.id)
        this.withLabel(record.label)
        record.properties.forEach { this.withProperty(it) }
        record.outgoingEdges.forEach { this.withEdge(Direction.OUT, it.label, it.record) }
        record.incomingEdges.forEach { this.withEdge(Direction.IN, it.label, it.record) }
        return this
    }

    fun withId(id: String): IVertexRecordBuilder {
        this.id = id
        return this
    }

    fun withLabel(label: String): IVertexRecordBuilder {
        this.label = label
        return this
    }

    fun withProperty(propertyKey: String, propertyValue: Any): IVertexRecordBuilder {
        this.properties[propertyKey] = VProp(propertyKey, propertyValue)
        return this
    }

    fun withProperty(propertyRecord: IVertexPropertyRecord): IVertexRecordBuilder {
        this.withProperty(propertyRecord.key, propertyRecord.value)
        propertyRecord.properties.values.forEach { metaProp -> this.withMetaProperty(propertyRecord.key, metaProp.key, metaProp.value) }
        return this
    }

    fun withProperty(property: VertexProperty<*>): IVertexRecordBuilder {
        this.withProperty(property.key(), property.value())
        property.properties<Any>().forEach { metaProp -> this.withMetaProperty(property.key(), metaProp.key(), metaProp.value()) }
        return this
    }

    fun withMetaProperty(propertyName: String, metaPropertyName: String, metaPropertyValue: Any): IVertexRecordBuilder {
        val vProp = this.properties[propertyName] ?: throw IllegalStateException("Cannot set meta property on property '${propertyName}': this property does not exist.")
        vProp.metaProps[metaPropertyName] = PropertyRecord2(metaPropertyName, metaPropertyValue)
        return this
    }

    fun withoutProperty(propertyName: String): IVertexRecordBuilder {
        this.properties.remove(propertyName)
        return this
    }

    fun withoutProperties(): IVertexRecordBuilder {
        this.properties.clear()
        return this
    }

    fun withEdge(direction: Direction, label: String, edgeId: String, otherEndVertexId: String): IVertexRecordBuilder {
        return this.withEdge(direction, label, EdgeTargetRecord2(edgeId, otherEndVertexId))
    }

    fun withEdge(direction: Direction, edgeTargetRecordWithLabel: EdgeTargetRecordWithLabel): IVertexRecordBuilder {
        return this.withEdge(direction, edgeTargetRecordWithLabel.label, edgeTargetRecordWithLabel.record.edgeId, edgeTargetRecordWithLabel.record.otherEndVertexId)
    }

    fun withEdge(direction: Direction, label: String, edgeTargetRecord: IEdgeTargetRecord): IVertexRecordBuilder {
        // make sure we have a "real" edge target record, if we don't, create a copy
        val e = edgeTargetRecord as? EdgeTargetRecord2 ?: EdgeTargetRecord2(edgeTargetRecord.edgeId, edgeTargetRecord.otherEndVertexId)
        when (direction) {
            Direction.IN -> this.inEdgeTargetRecords.put(label, e)
            Direction.OUT -> this.outEdgeTargetRecords.put(label, e)
            else -> throw IllegalArgumentException("Cannot add edge with direction BOTH!")
        }
        return this
    }

    fun withoutEdge(direction: Direction, label: String, edgeId: String, otherEndVertexId: String): IVertexRecordBuilder {
        when (direction) {
            Direction.IN -> this.inEdgeTargetRecords[label]?.removeIf { it.edgeId == edgeId && it.otherEndVertexId == otherEndVertexId }
            Direction.OUT -> this.outEdgeTargetRecords[label]?.removeIf { it.edgeId == edgeId && it.otherEndVertexId == otherEndVertexId }
            Direction.BOTH -> this.withoutEdge(Direction.IN, label, edgeId, otherEndVertexId).withoutEdge(Direction.OUT, label, edgeId, otherEndVertexId)
            else -> throw IllegalArgumentException("Unknown direction!")
        }
        return this
    }

    fun withoutEdgeToVertex(direction: Direction, label: String, otherEndVertexId: String): IVertexRecordBuilder {
        when (direction) {
            Direction.IN -> this.inEdgeTargetRecords[label]?.removeIf { it.otherEndVertexId == otherEndVertexId }
            Direction.OUT -> this.outEdgeTargetRecords[label]?.removeIf { it.otherEndVertexId == otherEndVertexId }
            Direction.BOTH -> this.withoutEdgeToVertex(Direction.IN, label, otherEndVertexId).withoutEdgeToVertex(Direction.OUT, label, otherEndVertexId)
            else -> throw IllegalArgumentException("Unknown direction!")
        }
        return this
    }

    fun withoutEdgeWithId(direction: Direction, label: String, edgeId: String): IVertexRecordBuilder {
        when (direction) {
            Direction.IN -> this.inEdgeTargetRecords[label]?.removeIf { it.edgeId == edgeId }
            Direction.OUT -> this.outEdgeTargetRecords[label]?.removeIf { it.edgeId == edgeId }
            Direction.BOTH -> this.withoutEdgeWithId(Direction.IN, label, edgeId).withoutEdgeWithId(Direction.OUT, label, edgeId)
            else -> throw IllegalArgumentException("Unknown direction: ${direction}!")
        }
        return this
    }

    fun withoutEdges(direction: Direction, label: String): IVertexRecordBuilder {
        when (direction) {
            Direction.IN -> this.inEdgeTargetRecords.removeAll(label)
            Direction.OUT -> this.outEdgeTargetRecords.removeAll(label)
            Direction.BOTH -> this.withoutEdges(Direction.IN, label).withoutEdges(Direction.OUT, label)
            else -> throw IllegalArgumentException("Unknown direction: ${direction}!")
        }
        return this
    }

    fun withoutEdges(direction: Direction): IVertexRecordBuilder {
        when (direction) {
            Direction.IN -> this.inEdgeTargetRecords.clear()
            Direction.OUT -> this.outEdgeTargetRecords.clear()
            Direction.BOTH -> this.withoutEdges(Direction.IN).withoutEdges(Direction.OUT)
            else -> throw IllegalArgumentException("Unknown direction: ${direction}!")
        }
        return this
    }

    fun build(): IVertexRecord {
        if (this.id == null) {
            throw IllegalStateException("Cannot create IVertexRecord without an ID!")
        }
        if (this.label == null) {
            throw IllegalStateException("Cannot create IVertexRecord without a label!")
        }
        // property keys must be unique
        val propertyKeyList = this.properties.values.asSequence().map { it.key }.toList()
        val distinctPropertyKeys = propertyKeyList.distinct()
        if (propertyKeyList.size != distinctPropertyKeys.size) {
            throw IllegalArgumentException("Cannot use the same VertexProperty key multiple times!")
        }
        // out edges must have unique IDs
        val outEdgeIdList = this.outEdgeTargetRecords.values().asSequence().map { it.edgeId }.toList()
        val distinctOutEdgeIds = outEdgeIdList.distinct()
        if (outEdgeIdList.size != distinctOutEdgeIds.size) {
            throw IllegalArgumentException("Cannot have two outgoing Edges with the same ID!")
        }
        // in edges must have unique IDs
        val inEdgeIdList = this.inEdgeTargetRecords.values().asSequence().map { it.edgeId }.toList()
        val distinctInEdgeIds = inEdgeIdList.distinct()
        if (inEdgeIdList.size != distinctInEdgeIds.size) {
            throw IllegalArgumentException("Cannot have two incoming Edges with the same ID!")
        }
        // if self-reference exists in out-edges, it must also exist in in-edges
        val outSelfEdges = this.outEdgeTargetRecords.entries().asSequence().filter { it.value.otherEndVertexId == this.id }.map { EdgeTargetRecordWithLabel(it.value, it.key) }.toSet()
        for (outSelfEdge in outSelfEdges) {
            val inEdgeExists = this.inEdgeTargetRecords.get(outSelfEdge.label).asSequence().filter { it.edgeId == outSelfEdge.record.edgeId && it.otherEndVertexId == this.id }.any()
            if (!inEdgeExists) {
                throw IllegalStateException("There is an outgoing self-edge (ID: '${outSelfEdge.record.edgeId}', label: '${outSelfEdge.label}') which is not listed as in-edge!")
            }
        }
        // if self-reference exists in in-edges, it must also exist in out-edges
        val inSelfEdges = this.inEdgeTargetRecords.entries().asSequence().filter { it.value.otherEndVertexId == this.id }.map { EdgeTargetRecordWithLabel(it.value, it.key) }.toSet()
        for (inSelfEdge in inSelfEdges) {
            val outEdgeExists = this.outEdgeTargetRecords.get(inSelfEdge.label).asSequence().filter { it.edgeId == inSelfEdge.record.edgeId && it.otherEndVertexId == this.id }.any()
            if (!outEdgeExists) {
                throw IllegalStateException("There is an incoming self-edge (ID: '${inSelfEdge.record.edgeId}', label: '${inSelfEdge.label}') which is not listed as out-edge!")
            }
        }
        // convert the properties to the internal format
        val convertedProperties = this.properties.asSequence().map { e -> e.value.toVertexPropertyRecord() }.toSet()
        return VertexRecord3(id, label, this.inEdgeTargetRecords, this.outEdgeTargetRecords, convertedProperties)
    }

    // =================================================================================================================
    // INNER CLASSES
    // =================================================================================================================

    private class VProp(
        val key: String,
        var value: Any,
        val metaProps: MutableMap<String, PropertyRecord2> = mutableMapOf()
    ) {

        fun toVertexPropertyRecord(): IVertexPropertyRecord {
            if(this.metaProps.isEmpty()){
                return SimpleVertexPropertyRecord(this.key, this.value)
            }
            return VertexPropertyRecord3(this.key, this.value, metaProps)
        }
    }

}
