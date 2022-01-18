package org.chronos.chronodb.internal.impl.index

import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.BranchManager
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.internal.impl.index.tree.IndexTreeImpl

interface IndexTree {

    companion object {

        @JvmStatic
        fun create(branchManager: BranchManager, existingIndices: Set<SecondaryIndex>): IndexTree {
            return IndexTreeImpl(
                existingIndices = existingIndices,
                getBranchByName = branchManager::getBranch,
                getChildBranches = branchManager::getChildBranches
            )
        }

    }

    fun getAllIndices(): Set<SecondaryIndex>

    fun getDirectChildren(index: SecondaryIndex): Set<SecondaryIndex>

    fun getDirectOrTransitiveChildren(index: SecondaryIndex): Set<SecondaryIndex>

    fun getIndexById(id: String): SecondaryIndex?

    fun getIndices(branch: Branch): Set<SecondaryIndex>

    fun getIndices(branch: Branch, timestamp: Long): Set<SecondaryIndex>

    fun getParentIndexOnBranch(index: SecondaryIndex, branch: Branch): SecondaryIndex

    fun getParentIndicesRecursive(index: SecondaryIndex, includeSelf: Boolean): List<SecondaryIndex>

    fun addIndex(index: SecondaryIndex): IndexChanges

    fun removeIndex(index: SecondaryIndex): IndexChanges

    fun changeValidityPeriodUpperBound(index: SecondaryIndex, newUpperBound: Long): IndexChanges

    fun onBranchCreated(branch: Branch): IndexChanges

    fun onBranchDeleted(branch: Branch): IndexChanges

    fun clear()

    fun isEmpty(): Boolean


}

