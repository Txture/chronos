package org.chronos.chronodb.internal.impl.index.tree

import com.google.common.annotations.VisibleForTesting
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.exceptions.ChronoDBIndexingException
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.index.IndexChanges
import org.chronos.chronodb.internal.impl.index.IndexTree
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl
import org.chronos.chronodb.internal.impl.index.StandardIndexingOption
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import kotlin.math.min

class IndexTreeImpl(
    existingIndices: Set<SecondaryIndex>,
    private val getBranchByName: (name: String) -> Branch?,
    private val getChildBranches: (parent: Branch, recursive: Boolean) -> List<Branch>
) : IndexTree {

    private val nodesById = mutableMapOf<String, IndexTreeNode>()

    private val nodesByBranch = mutableMapOf<Branch, MutableSet<IndexTreeNode>>()

    init {
        // check for overlaps
        val indicesByNameAndBranch = existingIndices.groupBy { it.name to it.branch }
        for (potentiallyOverlappingIndices in indicesByNameAndBranch.values) {
            for (index in potentiallyOverlappingIndices) {
                val overlappingIndex = potentiallyOverlappingIndices.asSequence()
                    .filterNot { it.id == index.id }
                    .filter { it.validPeriod.overlaps(index.validPeriod) }
                    .firstOrNull()
                if (overlappingIndex != null) {
                    throw ChronoDBIndexingException(
                        "The given indices are inconsistent: Index '${index}' is bound to" +
                            " the same branch and name as index '${overlappingIndex}' and overlaps" +
                            " with its validity period!"
                    )
                }
            }
        }

        val indicesByBranchName = existingIndices.groupBy { it.branch }
        val branchesByName = indicesByBranchName.keys.map { branchName ->
            val branch = this.getBranchByName(branchName)
            // throw an error if one of the mentioned branches doesn't exist
            if (branch == null) {
                // find out which index mentioned this branch
                val exampleIndex = existingIndices.first { it.branch == branchName }
                throw ChronoDBIndexingException(
                    "The given indices are inconsistent: Index '${exampleIndex}' references" +
                        " branch '${branchName}' which does not exist!"
                )
            }
            branchName to branch
        }.toMap()

        val indicesById = existingIndices.associateBy { it.id }
        val parentToChildren = existingIndices.groupBy {
            val parentId = it.parentIndexId
                ?: return@groupBy null
            val parentIndex = indicesById[parentId]
                ?: throw ChronoDBIndexingException(
                    "The given indices are inconsistent: Index '${it}' references" +
                        " a parent index '${parentId}' which does not exist!"
                )
            parentIndex
        }
        // create the root nodes
        val rootIndices = parentToChildren[null] ?: emptyList()
        if (rootIndices.isEmpty() && parentToChildren.isNotEmpty()) {
            throw ChronoDBIndexingException(
                "The given indices are inconsistent: There are inherited indices," +
                    " but no non-inherited indices!"
            )
        }

        // sort the indices, parent-first
        val sortedIndices = this.orderIndicesParentBeforeChild(existingIndices)

        for (index in sortedIndices) {
            val parentId = index.parentIndexId
            val parentNode = if (parentId == null) {
                null
            } else {
                this.nodesById[parentId]
                    ?: throw IllegalArgumentException(
                        "The given set of secondary indices is inconsistent - index '$index' references non-existing parent '$parentId'!"
                    )
            }
            val node = IndexTreeNode(index, parentNode)
            this.nodesById[index.id] = node
            val branch = branchesByName.getValue(index.branch)
            this.nodesByBranch.getOrPut(branch, ::mutableSetOf).add(node)
        }
    }

    override fun getAllIndices(): Set<SecondaryIndex> {
        return this.nodesById.values.asSequence().map { it.index }.toSet()
    }

    override fun getDirectChildren(index: SecondaryIndex): Set<SecondaryIndex> {
        val node = this.nodesById[index.id]
            ?: return emptySet()
        return node.children.asSequence().map { it.index }.toSet()
    }

    override fun getDirectOrTransitiveChildren(index: SecondaryIndex): Set<SecondaryIndex> {
        val node = this.nodesById[index.id]
            ?: return emptySet()
        return node.getDirectOrTransitiveChildren { it.index }
    }

    override fun getIndexById(id: String): SecondaryIndex? {
        return this.nodesById[id]?.index
    }

    override fun getIndices(branch: Branch): Set<SecondaryIndex> {
        val nodes = this.nodesByBranch[branch] ?: emptySet()
        return nodes.asSequence().map { it.index }.toSet()
    }

    override fun getIndices(branch: Branch, timestamp: Long): Set<SecondaryIndex> {
        // TODO [Performance]: Maybe use a hash map here instead of scanning linearly.
        val nodes = this.nodesByBranch[branch] ?: emptySet()
        return nodes.asSequence()
            .map { it.index }
            .filter { it.validPeriod.contains(timestamp) }
            .toSet()
    }

    override fun getParentIndexOnBranch(index: SecondaryIndex, branch: Branch): SecondaryIndex {
        var node = this.nodesById[index.id]
            ?: throw IllegalArgumentException("The index with ID '${index}' is unknown!")
        while (node.index.branch != branch.name) {
            node = node.parent
                ?: throw IllegalArgumentException("Could not find parent index '${index.name}' for branch '${branch.name}'!")
        }
        return node.index
    }

    override fun getParentIndicesRecursive(index: SecondaryIndex, includeSelf: Boolean): List<SecondaryIndex> {
        var node = this.nodesById[index.id]
            ?: throw IllegalArgumentException("The index with ID '${index}' is unknown!")
        val resultList = mutableListOf<SecondaryIndex>()
        if (includeSelf) {
            resultList.add(index)
        }
        while (node.parent != null) {
            node = node.parent!!
            resultList += node.index
        }
        return resultList
    }

    override fun addIndex(index: SecondaryIndex): IndexChanges {
        if (this.getIndexById(index.id) != null) {
            return IndexChanges.EMPTY
        }
        // add the index itself
        this.addIndexInternalNoCascade(index)
        val addedIndices = mutableSetOf<SecondaryIndex>()
        addedIndices.add(index)

        // cascade for child branches (if any)
        val branch = this.getBranchByName(index.branch)
            ?: throw IllegalArgumentException("The index '${index}' references a branch named '${index.branch}' which does not exist!")

        val childBranches = this.getChildBranches(branch, false).filter { index.validPeriod.contains(it.branchingTimestamp) }
        val toVisit = Stack<Pair<SecondaryIndex, Branch>>()
        childBranches.forEach { toVisit.push(index to it) }
        while (toVisit.isNotEmpty()) {
            val (parentIndex, currentBranch) = toVisit.pop()
            val clashingIndex = this.getIndices(currentBranch).asSequence()
                .filter { it.name == parentIndex.name }
                .minByOrNull { it.validPeriod.lowerBound }
            val indexStartsDirty = when {
                index.options.contains(StandardIndexingOption.ASSUME_NO_PRIOR_VALUES) -> false
                else -> true
            }
            val childIndex = if (clashingIndex == null) {
                SecondaryIndexImpl(
                    id = UUID.randomUUID().toString(),
                    name = parentIndex.name,
                    indexer = parentIndex.indexer,
                    validPeriod = Period.createRange(currentBranch.branchingTimestamp, index.validPeriod.upperBound),
                    branch = currentBranch.name,
                    parentIndexId = parentIndex.id,
                    dirty = indexStartsDirty,
                    options = parentIndex.inheritableOptions
                )
            } else if (clashingIndex.validPeriod.lowerBound > currentBranch.branchingTimestamp) {
                SecondaryIndexImpl(
                    id = UUID.randomUUID().toString(),
                    name = parentIndex.name,
                    indexer = parentIndex.indexer,
                    validPeriod = Period.createRange(currentBranch.branchingTimestamp, clashingIndex.validPeriod.lowerBound),
                    branch = currentBranch.name,
                    parentIndexId = parentIndex.id,
                    dirty = indexStartsDirty,
                    options = parentIndex.inheritableOptions
                )
            } else {
                // there already exists an index with the same name from the branching timestamp onward
                // -> stop the propagation here.
                null
            }
            if (childIndex != null) {
                this.addIndexInternalNoCascade(childIndex)
                addedIndices.add(childIndex)
                val grandChildBranches = this.getChildBranches(currentBranch, false).filter { childIndex.validPeriod.contains(it.branchingTimestamp) }
                grandChildBranches.forEach { toVisit.push(childIndex to it) }
            }
        }
        return IndexChanges(addedIndices = addedIndices)
    }

    private fun addIndexInternalNoCascade(index: SecondaryIndex): IndexChanges {
        if (this.getIndexById(index.id) != null) {
            return IndexChanges.EMPTY
        }
        // check if the branch exists
        val branch = this.getBranchByName(index.branch)
            ?: throw IllegalArgumentException("The index '${index}' references a branch named '${index.branch}' which does not exist!")

        val overlappingIndex = this.getIndices(branch).asSequence()
            .filter { it.name == index.name }
            .filter { it.validPeriod.overlaps(index.validPeriod) }
            .firstOrNull()

        if (overlappingIndex != null) {
            throw ChronoDBIndexingException(
                "There already is an index on branch '${index.branch}' for name '${index.name}'" +
                    " which overlaps with the given index. Overlapping time periods for indices" +
                    " on the same index name are not allowed. Existing index: ${overlappingIndex}," +
                    " new index: ${index}"
            )
        }

        // resolve the parent
        val parentId = index.parentIndexId
        val parentNode = if (parentId != null) {
            this.nodesById[parentId]
                ?: throw IllegalArgumentException("Index ${index} references parent index ID '${index.parentIndexId}' which does not exist!")
        } else {
            null
        }
        // create the new node
        val newNode = IndexTreeNode(index, parentNode)
        // add to index
        this.nodesById[index.id] = newNode
        this.nodesByBranch.getOrPut(branch, ::mutableSetOf).add(newNode)
        return IndexChanges(
            addedIndices = setOf(index),
            removedIndices = emptySet(),
            updatedIndices = emptySet()
        )
    }

    override fun removeIndex(index: SecondaryIndex): IndexChanges {
        val rootNode = this.nodesById[index.id]
            ?: return IndexChanges.EMPTY

        val childNodes = rootNode.getDirectOrTransitiveChildren { it }
        // remove our node from the parent (if any)
        rootNode.parent?.children?.remove(rootNode)

        val allRemovedNodes = mutableSetOf<IndexTreeNode>()
        allRemovedNodes.add(rootNode)
        allRemovedNodes.addAll(childNodes)

        for (node in allRemovedNodes) {
            val indexToRemove = node.index
            // remove our node from the index maps
            this.nodesById.remove(indexToRemove.id)
            val branch = this.getBranchByName(indexToRemove.branch)
            if (branch != null) {
                this.nodesByBranch[branch]?.remove(node)
            }
        }

        return IndexChanges(removedIndices = allRemovedNodes.asSequence().map { it.index }.toSet())
    }

    override fun changeValidityPeriodUpperBound(index: SecondaryIndex, newUpperBound: Long): IndexChanges {
        val node = this.nodesById[index.id]
            ?: throw IllegalArgumentException("The given index (${index}) is not managed by ChronoDB. Has it been removed?")
        val oldValidPeriod = node.index.validPeriod
        if (oldValidPeriod.upperBound == newUpperBound) {
            return IndexChanges.EMPTY
        }
        if (oldValidPeriod.lowerBound >= newUpperBound) {
            throw IllegalArgumentException(
                "The given newUpperBound (${newUpperBound}) is invalid:" +
                    " it is less than or equal to the lowerBound" +
                    " (${oldValidPeriod.lowerBound})!"
            )
        }
        val newValidPeriod = oldValidPeriod.setUpperBound(newUpperBound)
        // cascade
        var overallChanges = IndexChanges(updatedIndices = setOf(node.index))
        val branch = this.getBranchByName(node.index.branch)
            ?: throw IllegalStateException("There is no branch named '${node.index.branch}'!")
        if (oldValidPeriod.upperBound < newUpperBound) {
            // valid period is being extended. Check if we can do that without causing overlaps.
            val overlappingIndex = this.getIndices(branch).asSequence()
                .filterNot { it.id == node.index.id }
                .filter { it.name == node.index.name }
                .filter { it.validPeriod.overlaps(newValidPeriod) }
                .firstOrNull()
            if (overlappingIndex != null) {
                throw ChronoDBIndexingException(
                    "Cannot extend validity of index '${node.index}' to upper bound ${newUpperBound} because" +
                        " it would cause an overlap with existing index '${overlappingIndex}'!"
                )
            }
            val additionalPeriod = Period.createRange(oldValidPeriod.upperBound, newUpperBound)
            val additionalBranches = this.getChildBranches(branch, false).asSequence()
                .filter { additionalPeriod.contains(it.branchingTimestamp) }
                .toSet()
            for (additionalBranch in additionalBranches) {
                val clashingIndex = this.getIndices(additionalBranch).asSequence()
                    .filter { it.name == node.index.name }
                    .minByOrNull { it.validPeriod.lowerBound }
                val childIndex = if (clashingIndex == null) {
                    SecondaryIndexImpl(
                        id = UUID.randomUUID().toString(),
                        name = node.index.name,
                        indexer = node.index.indexer,
                        validPeriod = Period.createRange(additionalBranch.branchingTimestamp, newUpperBound),
                        branch = additionalBranch.name,
                        parentIndexId = node.index.id,
                        dirty = true,
                        options = node.index.inheritableOptions
                    )
                } else if (clashingIndex.validPeriod.lowerBound > additionalBranch.branchingTimestamp) {
                    SecondaryIndexImpl(
                        id = UUID.randomUUID().toString(),
                        name = node.index.name,
                        indexer = node.index.indexer,
                        validPeriod = Period.createRange(
                            additionalBranch.branchingTimestamp,
                            min(clashingIndex.validPeriod.lowerBound, newUpperBound)
                        ),
                        branch = additionalBranch.name,
                        parentIndexId = node.index.id,
                        dirty = true,
                        options = node.index.inheritableOptions
                    )
                } else {
                    // there already exists an index with the same name from the branching timestamp onward
                    // -> stop the propagation here.
                    null
                }
                if (childIndex != null) {
                    overallChanges = overallChanges.addAll(this.addIndex(childIndex))
                }
            }
        } else {
            // valid period is being shortened
            // The only thing we need to be careful about here is that a branching timestamp
            // which was in the old period might not be in the new period anymore, so we'd have
            // to delete the inherited indices on those "lost" branches.

            val directChildBranches = this.getChildBranches(branch, false)
            // those branches WERE in our valid period before, but are no longer in it now.
            val lostChildBranchNames = directChildBranches.asSequence()
                .filterNot { newValidPeriod.contains(it.branchingTimestamp) }
                .map { it.name }
                .toSet()
            val lostChildNodes = node.children.asSequence()
                .filter { it.index.branch in lostChildBranchNames }
                .toSet()
            for (childNode in lostChildNodes) {
                overallChanges = overallChanges.addAll(this.removeIndex(childNode.index))
            }
        }
        // update the valid period of the index
        (node.index as SecondaryIndexImpl).validPeriod = newValidPeriod
        return overallChanges
    }

    override fun onBranchCreated(branch: Branch): IndexChanges {
        val parentBranch = this.getBranchByName(branch.metadata.parentName)
        val indicesOnParentBranch = this.nodesByBranch[parentBranch]
            ?: return IndexChanges.EMPTY
        val branchingTimestamp = branch.metadata.branchingTimestamp
        val indicesToInherit = indicesOnParentBranch.asSequence()
            .filter { it.index.validPeriod.contains(branchingTimestamp) }
            .map { it.index }
            .toSet()
        if (indicesToInherit.isEmpty()) {
            return IndexChanges.EMPTY
        }
        var overallChanges = IndexChanges()
        // this branch is new, therefore there cannot be any indices defined
        // -> we don't need to check for clashing indices, we can simply add our inherited indices.
        for (parentIndex in indicesToInherit) {
            val childIndex = SecondaryIndexImpl(
                id = UUID.randomUUID().toString(),
                name = parentIndex.name,
                indexer = parentIndex.indexer,
                validPeriod = Period.createRange(branch.branchingTimestamp, parentIndex.validPeriod.upperBound),
                branch = branch.name,
                parentIndexId = parentIndex.id,
                // an inherited index holds the delta on the branch. Since we just
                // created the branch, it holds no commits. Whether it is dirty
                // therefore only depends on the dirty state of the parent index.
                dirty = parentIndex.dirty,
                options = parentIndex.inheritableOptions
            )
            overallChanges = overallChanges.addAll(this.addIndex(childIndex))
        }
        return overallChanges
    }

    override fun onBranchDeleted(branch: Branch): IndexChanges {
        val indexTreeNodes = this.nodesByBranch[branch]?.toSet() // we don't want a mutableSet
            ?: emptySet()
        var changes = IndexChanges.EMPTY
        for (indexToRemove in indexTreeNodes) {
            changes = changes.addAll(this.removeIndex(indexToRemove.index))
        }
        return changes
    }

    override fun clear() {
        this.nodesById.clear()
        this.nodesByBranch.clear()
    }

    override fun isEmpty(): Boolean {
        return this.nodesById.isEmpty()
    }

    @VisibleForTesting
    @Suppress("unused")
    fun printDebug() {
        val toVisit = Stack<Pair<Int, IndexTreeNode>>()
        this.nodesById.values.asSequence()
            .filter { it.parent == null }
            .sortedBy { it.index.branch + " " + it.index.name }
            .forEach { toVisit.push(0 to it) }
        while (toVisit.isNotEmpty()) {
            val (indentationDepth, node) = toVisit.pop()
            println("${"    ".repeat(indentationDepth)}${node}")
            node.children.asSequence().sortedBy { it.index.branch + " " + it.index.name }.forEach { toVisit.push(Pair(indentationDepth + 1, it)) }
        }
    }

    private fun orderIndicesParentBeforeChild(indices: Set<SecondaryIndex>): List<SecondaryIndex> {
        if (indices.isEmpty()) {
            return emptyList()
        }
        val parentIdToChildIndices = indices.groupBy { it.parentIndexId }
        val masterIndices = parentIdToChildIndices[null] ?: emptyList()
        val resultList = mutableListOf<SecondaryIndex>()
        val toVisit = LinkedBlockingQueue<SecondaryIndex>()
        masterIndices.forEach(toVisit::put)
        while (toVisit.isNotEmpty()) {
            val currentIndex = toVisit.poll()
            resultList.add(currentIndex)
            val children = parentIdToChildIndices[currentIndex.id] ?: emptyList()
            children.forEach(toVisit::put)
        }
        val inconsistent = indices.toMutableSet()
        inconsistent.removeAll(resultList)
        if (inconsistent.isNotEmpty()) {
            throw IllegalArgumentException(
                "The given set of secondary indices is inconsistent, as the indices do not form an inheritance tree!" +
                    " Inconsistent indices: ${inconsistent}"
            )
        }

        return resultList
    }
}