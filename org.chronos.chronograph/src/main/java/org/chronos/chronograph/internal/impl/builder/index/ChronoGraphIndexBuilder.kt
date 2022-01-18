package org.chronos.chronograph.internal.impl.builder.index

import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.index.IndexingOption
import org.chronos.chronograph.api.builder.index.*
import org.chronos.chronograph.api.index.ChronoGraphIndex
import org.chronos.chronograph.internal.api.index.ChronoGraphIndexManagerInternal
import org.chronos.chronograph.internal.impl.index.IndexType

class ChronoGraphIndexBuilder(
    private val manager: ChronoGraphIndexManagerInternal
) : IndexBuilderStarter,
    ElementTypeChoiceIndexBuilder,
    VertexIndexBuilder,
    FinalizableVertexIndexBuilder,
    EdgeIndexBuilder,
    FinalizableEdgeIndexBuilder {

    private var indexType: IndexType = IndexType.STRING
    private var elementClass: Class<out Element> = Vertex::class.java
    private var startTimestamp: Long = 0
    private var endTimestamp: Long = Long.MAX_VALUE
    private var propertyName: String = ""
    private var options = mutableSetOf<IndexingOption>()


    override fun stringIndex(): ElementTypeChoiceIndexBuilder {
        this.indexType = IndexType.STRING
        return this
    }

    override fun longIndex(): ElementTypeChoiceIndexBuilder {
        this.indexType = IndexType.LONG
        return this
    }

    override fun doubleIndex(): ElementTypeChoiceIndexBuilder {
        this.indexType = IndexType.DOUBLE
        return this
    }

    override fun withValidityPeriod(period: Period): ChronoGraphIndexBuilder {
        return this.withValidityPeriod(period.lowerBound, period.upperBound)
    }

    override fun withValidityPeriod(startTimestamp: Long, endTimestamp: Long): ChronoGraphIndexBuilder {
        require(startTimestamp >= 0) { "Argument 'startTimestamp' must not be negative!" }
        require(endTimestamp > startTimestamp) { "Argument 'endTimestamp' (value: ${endTimestamp}) must be greater than 'startTimestamp' (value: ${startTimestamp})!" }
        this.startTimestamp = startTimestamp
        this.endTimestamp = endTimestamp
        return this
    }

    override fun acrossAllTimestamps(): ChronoGraphIndexBuilder {
        return this.withValidityPeriod(Period.eternal())
    }

    override fun onVertexProperty(propertyName: String): VertexIndexBuilder {
        require(propertyName.isNotEmpty()) { "Property names for indices must not be empty!" }
        this.propertyName = propertyName
        this.elementClass = Vertex::class.java
        return this
    }

    override fun onEdgeProperty(propertyName: String): EdgeIndexBuilder {
        require(propertyName.isNotEmpty()) { "Property names for indices must not be empty!" }
        this.propertyName = propertyName
        this.elementClass = Edge::class.java
        return this
    }

    override fun assumeNoPriorValues(assumeNoPriorValues: Boolean): ChronoGraphIndexBuilder {
        if(assumeNoPriorValues){
            this.options.add(IndexingOption.assumeNoPriorValues())
        }else{
            this.options.remove(IndexingOption.assumeNoPriorValues())
        }
        return this
    }

    override fun build(): ChronoGraphIndex {
        return this.manager.addIndex(this.elementClass, this.indexType, this.propertyName, this.startTimestamp, this.endTimestamp, this.options)
    }

}