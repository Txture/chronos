package org.chronos.chronodb.internal.impl.index

import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.indexing.Indexer
import org.chronos.chronodb.internal.api.Period

class SecondaryIndexImpl(
        override val id: String,
        override val name: String,
        override val indexer: Indexer<*>,
        override var validPeriod: Period,
        override val branch: String,
        override val parentIndexId: String?,
        override var dirty: Boolean,
        override val options: Set<IndexingOption>
) : SecondaryIndex {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SecondaryIndexImpl

        if (id != other.id) return false

        return true
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun toString(): String {
        return "SecondaryIndexImpl(" +
                "id='$id', " +
                "name='$name', " +
                "validPeriod=$validPeriod, " +
                "branch='$branch', " +
                "parentIndexId='$parentIndexId', " +
                "dirty=$dirty)"
    }


}