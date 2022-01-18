package org.chronos.chronodb.api

import org.chronos.chronodb.api.indexing.DoubleIndexer
import org.chronos.chronodb.api.indexing.Indexer
import org.chronos.chronodb.api.indexing.LongIndexer
import org.chronos.chronodb.api.indexing.StringIndexer
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.api.index.IndexManagerInternal
import org.chronos.chronodb.internal.impl.index.IndexingOption

interface IndexBuilder {

    fun withName(name: String): IndexBuilderWithName

}

interface IndexBuilderWithName {

    fun withIndexer(indexer: StringIndexer): IndexBuilderWithIndexer

    fun withIndexer(indexer: LongIndexer): IndexBuilderWithIndexer

    fun withIndexer(indexer: DoubleIndexer): IndexBuilderWithIndexer

    fun withIndexer(indexer: Indexer<*>): IndexBuilderWithIndexer {
        return when (indexer) {
            is StringIndexer -> this.withIndexer(indexer)
            is LongIndexer -> this.withIndexer(indexer)
            is DoubleIndexer -> this.withIndexer(indexer)
            else -> throw IllegalArgumentException(
                "Indexer 'indexer' doesn't implement any of the required sub-interfaces [StringIndexer, LongIndexer, DoubleIndexer]!"
            )
        }
    }
}

interface IndexBuilderWithIndexer {

    fun onBranch(branch: String): IndexBuilderWithBranch

    fun onBranch(branch: Branch): IndexBuilderWithBranch {
        return this.onBranch(branch.name)
    }

    fun onMaster(): IndexBuilderWithBranch {
        return this.onBranch(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER)
    }

}

interface IndexBuilderWithBranch {

    fun fromTimestamp(startTimestamp: Long): IndexBuilderWithStartTimestamp

    fun withPeriod(period: Period): IndexBuilderWithPeriod

    fun acrossAllTimestamps(): IndexBuilderWithPeriod {
        return this.withPeriod(Period.eternal())
    }

}

interface IndexBuilderWithStartTimestamp {

    fun toTimestamp(endTimestamp: Long): IndexBuilderWithPeriod

    fun toInfinity(): IndexBuilderWithPeriod {
        return this.toTimestamp(Long.MAX_VALUE)
    }

}

interface IndexBuilderWithPeriod {

    fun withOption(option: IndexingOption): IndexBuilderWithPeriod

    fun withOptions(options: Collection<IndexingOption>): IndexBuilderWithPeriod {
        var chain: IndexBuilderWithPeriod = this
        for(option in options){
            chain = chain.withOption(option)
        }
        return chain
    }

    fun build(): SecondaryIndex

}

class IndexBuilderImpl(
    private val indexManager: IndexManagerInternal,
) : IndexBuilder,
    IndexBuilderWithName,
    IndexBuilderWithIndexer,
    IndexBuilderWithBranch,
    IndexBuilderWithStartTimestamp,
    IndexBuilderWithPeriod {

    private var indexName: String = ""
    private var startTimestamp: Long = -1
    private var endTimestamp: Long = -1
    private var branchName: String = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER
    private var indexer: Indexer<*>? = null
    private val options: MutableSet<IndexingOption> = mutableSetOf()

    override fun withName(name: String): IndexBuilderWithName {
        require(indexName.isEmpty()) { "Argument 'name' must not be empty!" }
        this.indexName = name
        return this
    }

    override fun withIndexer(indexer: StringIndexer): IndexBuilderWithIndexer {
        this.indexer = indexer
        return this
    }

    override fun withIndexer(indexer: LongIndexer): IndexBuilderWithIndexer {
        this.indexer = indexer
        return this
    }

    override fun withIndexer(indexer: DoubleIndexer): IndexBuilderWithIndexer {
        this.indexer = indexer
        return this
    }

    override fun onBranch(branch: String): IndexBuilderWithBranch {
        this.branchName = branch
        return this
    }

    override fun fromTimestamp(startTimestamp: Long): IndexBuilderWithStartTimestamp {
        require(startTimestamp >= 0) {
            "Argument 'startTimestamp' must not be negative (value: ${startTimestamp})!"
        }
        this.startTimestamp = startTimestamp
        return this
    }

    override fun withPeriod(period: Period): IndexBuilderWithPeriod {
        this.startTimestamp = period.lowerBound
        this.endTimestamp = period.upperBound
        return this
    }

    override fun toTimestamp(endTimestamp: Long): IndexBuilderWithPeriod {
        require(endTimestamp > this.startTimestamp) {
            "Argument 'endTimestamp' (value: ${endTimestamp}) must be greater than the given startTimestamp (value: ${this.startTimestamp})!"
        }
        this.endTimestamp = endTimestamp
        return this
    }

    override fun withOption(option: IndexingOption): IndexBuilderWithPeriod {
        this.options.add(option)
        return this
    }

    override fun build(): SecondaryIndex {
        return indexManager.addIndex(
            this.indexName,
            this.branchName,
            this.startTimestamp,
            this.endTimestamp,
            this.indexer!!,
            this.options
        )
    }


}