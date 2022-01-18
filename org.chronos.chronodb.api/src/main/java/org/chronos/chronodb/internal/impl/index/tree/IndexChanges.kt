package org.chronos.chronodb.internal.impl.index

import org.chronos.chronodb.api.SecondaryIndex

class IndexChanges(
    val addedIndices: Set<SecondaryIndex> = emptySet(),
    val removedIndices: Set<SecondaryIndex> = emptySet(),
    val updatedIndices: Set<SecondaryIndex> = emptySet()
) {

    companion object {

        val EMPTY = IndexChanges(emptySet(), emptySet(), emptySet())

    }

    fun addAll(changes: IndexChanges): IndexChanges {
        return IndexChanges(
            this.addedIndices + changes.addedIndices,
            this.removedIndices + changes.removedIndices,
            this.updatedIndices + changes.updatedIndices
        )
    }

}