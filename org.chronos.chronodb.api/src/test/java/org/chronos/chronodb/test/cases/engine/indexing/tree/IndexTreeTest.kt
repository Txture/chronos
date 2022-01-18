package org.chronos.chronodb.test.cases.engine.indexing.tree

import com.google.common.collect.Lists
import org.chronos.chronodb.api.Branch
import org.chronos.chronodb.api.ChronoDBConstants
import org.chronos.chronodb.api.SecondaryIndex
import org.chronos.chronodb.api.exceptions.ChronoDBIndexingException
import org.chronos.chronodb.internal.api.Period
import org.chronos.chronodb.internal.impl.IBranchMetadata
import org.chronos.chronodb.internal.impl.index.IndexTree
import org.chronos.chronodb.internal.impl.index.SecondaryIndexImpl
import org.chronos.chronodb.internal.impl.index.tree.IndexTreeImpl
import org.chronos.chronodb.test.cases.util.model.payload.NamedPayloadNameIndexer
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import java.util.*

class IndexTreeTest {

    @Test
    fun canAddAndRemoveIndexOnBranch() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )

        val indexTree = this.createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)

        val idxName = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub1.branchingTimestamp + 1000L),
            branch = sub1.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        val changesOnAdd = indexTree.addIndex(idxName)
        assertEquals(0, changesOnAdd.removedIndices.size)
        assertEquals(0, changesOnAdd.updatedIndices.size)
        assertEquals(
            setOf(
                "Sub1:name.${idxName.validPeriod.lowerBound}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}"
            ).sorted(),
            changesOnAdd.addedIndices.map { it.fingerprint }.sorted()
        )

        assertEquals(
            setOf(
                "Sub1:name.${idxName.validPeriod.lowerBound}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}"
            ).sorted(),
            indexTree.getAllIndices().map { it.fingerprint }.sorted()
        )

        val changesOnRemove = indexTree.removeIndex(idxName)
        assertEquals(0, changesOnRemove.addedIndices.size)
        assertEquals(0, changesOnRemove.updatedIndices.size)
        assertEquals(setOf(
            "Sub1:name.${idxName.validPeriod.lowerBound}",
            "Sub1a:name.${sub1a.branchingTimestamp}",
            "Sub1b:name.${sub1b.branchingTimestamp}"
        ).sorted(),
            changesOnRemove.removedIndices.map { it.fingerprint }.sorted())

        assertEquals(0, indexTree.getAllIndices().size)
        assertEquals(0, indexTree.getIndices(sub1).size)
        assertEquals(0, indexTree.getIndices(sub1a).size)
        assertEquals(0, indexTree.getIndices(sub1b).size)
    }

    @Test
    fun canCreateIndexTreeFromExistingIndices() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )
        val sub2a = sub2.createChild(
            name = "Sub2a",
            branchingTimestamp = master.now - 30000L,
            now = master.now
        )
        val sub2b = sub2.createChild(
            name = "Sub2b",
            branchingTimestamp = master.now - 20000L,
            now = master.now
        )
        val idxName = SecondaryIndexImpl(
            id = "name",
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )
        val idxNameChild1 = SecondaryIndexImpl(
            id = "name-child-1-root",
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub1.branchingTimestamp),
            branch = sub1.name,
            parentIndexId = idxName.id,
            dirty = true,
            options = emptySet()
        )
        val idxNameChild1a = SecondaryIndexImpl(
            id = "name-child-1a",
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub1a.branchingTimestamp),
            branch = sub1a.name,
            parentIndexId = idxNameChild1.id,
            dirty = true,
            options = emptySet()
        )
        val idxNameChild1b = SecondaryIndexImpl(
            id = "name-child-1b",
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub1b.branchingTimestamp),
            branch = sub1b.name,
            parentIndexId = idxNameChild1.id,
            dirty = true,
            options = emptySet()
        )
        val idxNameChild2 = SecondaryIndexImpl(
            id = "name-child-2-root",
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub2.branchingTimestamp),
            branch = sub2.name,
            parentIndexId = idxName.id,
            dirty = true,
            options = emptySet()
        )
        val idxNameChild2a = SecondaryIndexImpl(
            id = "name-child-2a",
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub2a.branchingTimestamp),
            branch = sub2a.name,
            parentIndexId = idxNameChild2.id,
            dirty = true,
            options = emptySet()
        )
        val idxNameChild2b = SecondaryIndexImpl(
            id = "name-child-2b",
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub2b.branchingTimestamp),
            branch = sub2b.name,
            parentIndexId = idxNameChild2.id,
            dirty = true,
            options = emptySet()
        )
        val idxDescription = SecondaryIndexImpl(
            id = "desc",
            name = "description",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(50000L, 1000000L),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )
        val idxFragment1 = SecondaryIndexImpl(
            id = "frag1",
            name = "fragment",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(40000L, 60000L),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )
        val idxFragment2 = SecondaryIndexImpl(
            id = "frag2",
            name = "fragment",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(100000L, Long.MAX_VALUE),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )
        val allBranches = setOf(
            master,
            sub1,
            sub1a,
            sub1b,
            sub2,
            sub2a,
            sub2b,
        )
        val allIndices = setOf(
            idxName,
            idxNameChild1,
            idxNameChild1a,
            idxNameChild1b,
            idxNameChild2,
            idxNameChild2a,
            idxNameChild2b,
            idxDescription,
            idxFragment1,
            idxFragment2,
        )

        val indexTree = this.createIndexTree(allBranches, allIndices)
        assertEquals(
            setOf(
                "master:name.0",
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}",
                "Sub2:name.${sub2.branchingTimestamp}",
                "Sub2a:name.${sub2a.branchingTimestamp}",
                "Sub2b:name.${sub2b.branchingTimestamp}",
                "master:description.50000",
                "master:fragment.40000",
                "master:fragment.100000"
            ).sorted(),
            indexTree.getAllIndices().map { it.fingerprint }.sorted()
        )

        assertEquals(
            setOf(
                "Sub1:name.${sub1.branchingTimestamp}"
            ).sorted(),
            indexTree.getIndices(sub1).map { it.fingerprint }.sorted()
        )
        assertEquals(
            setOf(
                "master:name.0",
                "master:description.50000",
                "master:fragment.40000",
                "master:fragment.100000"
            ).sorted(),
            indexTree.getIndices(master).map { it.fingerprint }.sorted()
        )
        assertEquals(
            setOf(
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}",
                "Sub2:name.${sub2.branchingTimestamp}",
                "Sub2a:name.${sub2a.branchingTimestamp}",
                "Sub2b:name.${sub2b.branchingTimestamp}",
            ).sorted(),
            indexTree.getDirectOrTransitiveChildren(idxName).map { it.fingerprint }.sorted()
        )
        assertEquals(
            setOf(
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub2:name.${sub2.branchingTimestamp}",
            ).sorted(),
            indexTree.getDirectChildren(idxName).map { it.fingerprint }.sorted()
        )
    }

    @Test
    fun canAddIndicesOnMaster() {
        val master = DummyBranch.MASTER
        val branchByName = mapOf(master.name to master)
        val indexTree = IndexTreeImpl(
            existingIndices = emptySet(),
            getBranchByName = branchByName::get
        ) { _, _ -> emptyList() }

        assertEquals(0, indexTree.getAllIndices().size)
        assertEquals(0, indexTree.getIndices(master).size)
        assertEquals(0, indexTree.getIndices(master, master.now - 10000L).size)

        // add a couple of indices
        val idxName = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )
        val idxDescription = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "description",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(master.now - 1000000L, master.now + 50000L),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )
        val idxFragment1 = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "fragment",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(master.now - 60000L, master.now - 40000L),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )
        val idxFragment2 = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "fragment",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(master.now - 30000L, Long.MAX_VALUE),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        indexTree.addIndex(idxName)
        indexTree.addIndex(idxDescription)
        indexTree.addIndex(idxFragment1)
        indexTree.addIndex(idxFragment2)

        assertEquals(4, indexTree.getAllIndices().size)
        assertEquals(4, indexTree.getIndices(master).size)
        assertEquals(3, indexTree.getIndices(master, master.now - 10000L).size)
    }

    @Test
    fun cannotAddClashingIndices() {
        val master = DummyBranch.MASTER
        val branchByName = mapOf(master.name to master)
        val indexTree = IndexTreeImpl(
            existingIndices = emptySet(),
            getBranchByName = branchByName::get
        ) { _, _ -> emptyList() }

        // add a couple of indices
        val idxName1 = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        // add a couple of indices
        val idxName2 = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(master.now - 1000L, master.now + 1000L),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        indexTree.addIndex(idxName1)
        try {
            indexTree.addIndex(idxName2)
            fail("Managed to add two indices on the same name with overlapping periods to index tree!")
        } catch (expected: ChronoDBIndexingException) {
            // pass
        }
    }

    @Test
    fun addingAnIndexInheritsItToChildBranches() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )

        val indexTree = createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)
        val globalNameIndex = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        val changes = indexTree.addIndex(globalNameIndex)
        assertEquals(emptySet<SecondaryIndex>(), changes.removedIndices)
        assertEquals(emptySet<SecondaryIndex>(), changes.updatedIndices)
        assertEquals(setOf(master.name, sub1.name, sub2.name, sub1a.name, sub1b.name), changes.addedIndices.map { it.branch }.toSet())

        // this index relates to a period which is BEFORE all branches.
        // No branch should be affected by this change.
        val masterOnlyIndex = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "description",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(100000, 200000),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        val changes2 = indexTree.addIndex(masterOnlyIndex)
        assertEquals(emptySet<SecondaryIndex>(), changes2.removedIndices)
        assertEquals(emptySet<SecondaryIndex>(), changes2.updatedIndices)
        assertEquals(setOf(master.name), changes2.addedIndices.map { it.branch }.toSet())

        // add an index that includes sub1, but not sub2
        val masterAndSub1Index = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "description",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(sub1.branchingTimestamp - 1000, sub2.branchingTimestamp - 1000),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        val changes3 = indexTree.addIndex(masterAndSub1Index)
        assertEquals(emptySet<SecondaryIndex>(), changes3.removedIndices)
        assertEquals(emptySet<SecondaryIndex>(), changes3.updatedIndices)
        assertEquals(setOf(master.name, sub1.name, sub1a.name, sub1b.name), changes3.addedIndices.map { it.branch }.toSet())

        val expected = listOf(
            "master:name.0",
            "master:description.100000",
            "master:description.${sub1.branchingTimestamp - 1000}",
            "Sub1:name.${sub1.branchingTimestamp}",
            "Sub1:description.${sub1.branchingTimestamp}",
            "Sub1a:name.${sub1a.branchingTimestamp}",
            "Sub1b:name.${sub1b.branchingTimestamp}",
            "Sub1a:description.${sub1a.branchingTimestamp}",
            "Sub1b:description.${sub1b.branchingTimestamp}",
            "Sub2:name.${sub2.branchingTimestamp}",
        ).sorted()
        val actual = indexTree.getAllIndices().map { it.fingerprint }.toSet().sorted()
        assertEquals(expected, actual)
    }

    @Test
    fun removingAnIndexAlsoRemovesChildren() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )

        val indexTree = createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)
        val globalNameIndex = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        val changes = indexTree.addIndex(globalNameIndex)
        assertEquals(emptySet<SecondaryIndex>(), changes.removedIndices)
        assertEquals(emptySet<SecondaryIndex>(), changes.updatedIndices)
        assertEquals(setOf(master.name, sub1.name, sub2.name, sub1a.name, sub1b.name), changes.addedIndices.map { it.branch }.toSet())

        // this index relates to a period which is BEFORE all branches.
        // No branch should be affected by this change.
        val masterOnlyIndex = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "description",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(100000, 200000),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        indexTree.addIndex(masterOnlyIndex)

        // add an index that includes sub1, but not sub2
        val masterAndSub1Index = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "description",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(sub1.branchingTimestamp - 1000, sub2.branchingTimestamp - 1000),
            branch = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
            parentIndexId = null,
            dirty = true,
            options = emptySet()
        )

        indexTree.addIndex(masterAndSub1Index)

        val changes1 = indexTree.removeIndex(masterAndSub1Index)
        assertEquals(0, changes1.addedIndices.size)
        assertEquals(0, changes1.updatedIndices.size)
        assertEquals(setOf(
            "master:description.${sub1.branchingTimestamp - 1000}",
            "Sub1:description.${sub1.branchingTimestamp}",
            "Sub1a:description.${sub1a.branchingTimestamp}",
            "Sub1b:description.${sub1b.branchingTimestamp}"
        ), changes1.removedIndices.map { it.fingerprint }.toSet())

        assertEquals(setOf(
            "master:name.0",
            "master:description.${100000}",
            "Sub1:name.${sub1.branchingTimestamp}",
            "Sub1a:name.${sub1a.branchingTimestamp}",
            "Sub1b:name.${sub1b.branchingTimestamp}",
            "Sub2:name.${sub2.branchingTimestamp}",
        ), indexTree.getAllIndices().map { it.fingerprint }.toSet())

        assertEquals(setOf(
            "master:name.0",
            "master:description.${100000}",
        ), indexTree.getIndices(master).map { it.fingerprint }.toSet())

        assertEquals(setOf(
            "Sub1:name.${sub1.branchingTimestamp}",
        ), indexTree.getIndices(sub1).map { it.fingerprint }.toSet())

        assertEquals(setOf(
            "Sub1a:name.${sub1a.branchingTimestamp}",
        ), indexTree.getIndices(sub1a).map { it.fingerprint }.toSet())

        assertEquals(setOf(
            "Sub1b:name.${sub1b.branchingTimestamp}",
        ), indexTree.getIndices(sub1b).map { it.fingerprint }.toSet())

        assertEquals(setOf(
            "Sub2:name.${sub2.branchingTimestamp}",
        ), indexTree.getIndices(sub2).map { it.fingerprint }.toSet())
    }

    @Test
    fun removingAChildIndexDoesNotModifyParentIndex() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )

        val indexTree = this.createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)

        val idxName = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub1.branchingTimestamp + 1000L),
            branch = sub1.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        val changesOnAdd = indexTree.addIndex(idxName)
        assertEquals(0, changesOnAdd.removedIndices.size)
        assertEquals(0, changesOnAdd.updatedIndices.size)
        assertEquals(
            setOf(
                "Sub1:name.${idxName.validPeriod.lowerBound}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}"
            ).sorted(),
            changesOnAdd.addedIndices.map { it.fingerprint }.sorted()
        )

        val idxNameSub1a = indexTree.getIndices(sub1a).single()
        val changesOnRemove = indexTree.removeIndex(idxNameSub1a)
        assertEquals(0, changesOnRemove.addedIndices.size)
        assertEquals(0, changesOnRemove.updatedIndices.size)
        assertEquals(setOf(idxNameSub1a), changesOnRemove.removedIndices)

        // make sure the index is gone
        assertEquals(0, indexTree.getIndices(sub1a).size)

        // make sure the other indices are still there
        assertEquals(
            setOf(
                "Sub1:name.${idxName.validPeriod.lowerBound}",
                "Sub1b:name.${sub1b.branchingTimestamp}"
            ).sorted(),
            indexTree.getAllIndices().map { it.fingerprint }.sorted()
        )
    }

    @Test
    fun changingChildIndexValidityDoesNotModifyParentIndex() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )

        val indexTree = this.createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)

        val idxName = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createOpenEndedRange(sub1.branchingTimestamp + 1000L),
            branch = sub1.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        val changesOnAdd = indexTree.addIndex(idxName)
        assertEquals(0, changesOnAdd.removedIndices.size)
        assertEquals(0, changesOnAdd.updatedIndices.size)
        assertEquals(
            setOf(
                "Sub1:name.${idxName.validPeriod.lowerBound}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}"
            ).sorted(),
            changesOnAdd.addedIndices.map { it.fingerprint }.sorted()
        )

        val idxNameSub1a = indexTree.getIndices(sub1a).single()
        val changesOnAlterPeriod = indexTree.changeValidityPeriodUpperBound(idxNameSub1a, sub1a.branchingTimestamp + 1000000L)
        assertEquals(0, changesOnAlterPeriod.addedIndices.size)
        assertEquals(0, changesOnAlterPeriod.removedIndices.size)
        assertEquals(setOf(idxNameSub1a), changesOnAlterPeriod.updatedIndices)

        assertEquals(sub1a.branchingTimestamp + 1000000L, idxNameSub1a.validPeriod.upperBound)

        // the other 2 indices should still have their original (unlimited) upper bounds
        assertEquals(
            listOf(Long.MAX_VALUE, Long.MAX_VALUE),
            indexTree.getAllIndices().filter { it != idxNameSub1a }.map { it.validPeriod.upperBound }.toList()
        )
    }

    @Test
    fun extendingValidityAcrossBranchingTimestampsWorks() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100_000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50_000L,
            now = master.now
        )
        val indexTree = this.createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)

        val idxName = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(0, sub1.branchingTimestamp - 1L),
            branch = master.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )

        val changesAdd = indexTree.addIndex(idxName)
        assertEquals(0, changesAdd.removedIndices.size)
        assertEquals(0, changesAdd.updatedIndices.size)
        assertEquals(setOf(idxName), changesAdd.addedIndices)

        val changesOnExtend1 = indexTree.changeValidityPeriodUpperBound(idxName, sub1b.branchingTimestamp + 1L)
        assertEquals(
            setOf(
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}"
            ).sorted(),
            changesOnExtend1.addedIndices.map { it.fingerprint }.sorted()
        )
        assertEquals(0, changesOnExtend1.removedIndices.size)
        assertEquals(setOf(idxName), changesOnExtend1.updatedIndices)

        val changesOnExtend2 = indexTree.changeValidityPeriodUpperBound(idxName, sub2.branchingTimestamp + 1L)
        assertEquals(
            setOf(
                "Sub2:name.${sub2.branchingTimestamp}"
            ).sorted(),
            changesOnExtend2.addedIndices.map { it.fingerprint }.sorted()
        )
        assertEquals(0, changesOnExtend2.removedIndices.size)
        assertEquals(setOf(idxName), changesOnExtend2.updatedIndices)
    }

    @Test
    fun shrinkingValidityAcrossBranchingTimestampsWorks() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )
        val indexTree = this.createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)
        val idxName = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = master.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        val changesOnAdd = indexTree.addIndex(idxName)
        assertEquals(
            setOf(
                "master:name.0",
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}",
                "Sub2:name.${sub2.branchingTimestamp}",
            ).sorted(),
            changesOnAdd.addedIndices.map { it.fingerprint }.sorted()
        )
        assertEquals(0, changesOnAdd.removedIndices.size)
        assertEquals(0, changesOnAdd.updatedIndices.size)

        assertEquals(
            setOf(
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub2:name.${sub2.branchingTimestamp}",
            ).sorted(),
            indexTree.getDirectChildren(idxName).map { it.fingerprint }.sorted()
        )

        assertEquals(
            setOf(
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}",
                "Sub2:name.${sub2.branchingTimestamp}"
            ).sorted(),
            indexTree.getDirectOrTransitiveChildren(idxName).map { it.fingerprint }.sorted()
        )

        // limit the validity of index "name" on master to be below the
        // branching timestamp of sub2. This should remove the inherited "name"
        // index from branch sub2.

        val changesOnShrink1 = indexTree.changeValidityPeriodUpperBound(idxName, sub2.branchingTimestamp - 1)
        assertEquals(0, changesOnShrink1.addedIndices.size)
        assertEquals(
            setOf(
                "Sub2:name.${sub2.branchingTimestamp}"
            ).sorted(),
            changesOnShrink1.removedIndices.map { it.fingerprint }.sorted()
        )
        assertEquals(setOf(idxName), changesOnShrink1.updatedIndices)

        // the index on sub2 should be gone now
        assertEquals(0, indexTree.getIndices(sub2).size)
        // the other indices should still exist
        assertEquals(
            setOf(
                "master:name.0",
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}"
            ).sorted(),
            indexTree.getAllIndices().map { it.fingerprint }.sorted()
        )

        // if we shrink the index further, past the indexing timestamp of sub1, we should
        // get rid of the indices on sub1, sub1a and sub1b (cascading)

        val changesOnShrink2 = indexTree.changeValidityPeriodUpperBound(idxName, sub1.branchingTimestamp - 1)
        assertEquals(0, changesOnShrink2.addedIndices.size)
        assertEquals(
            setOf(
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub1a:name.${sub1a.branchingTimestamp}",
                "Sub1b:name.${sub1b.branchingTimestamp}"
            ).sorted(),
            changesOnShrink2.removedIndices.map { it.fingerprint }.sorted()
        )
        assertEquals(setOf(idxName), changesOnShrink2.updatedIndices)

        // the index on sub1 should be gone now
        assertEquals(0, indexTree.getIndices(sub1).size)
        assertEquals(0, indexTree.getIndices(sub1a).size)
        assertEquals(0, indexTree.getIndices(sub1b).size)

        // the index on master should still exist
        assertEquals(setOf(idxName), indexTree.getIndices(master))

    }

    @Test
    fun inheritanceIsBlockedByExistingIndices1() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )
        val indexTree = this.createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)
        val idxNameSub1 = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(sub1.branchingTimestamp, sub1a.branchingTimestamp - 1),
            branch = sub1.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        val changesIdxNameSub1Added = indexTree.addIndex(idxNameSub1)
        assertEquals(0, changesIdxNameSub1Added.updatedIndices.size)
        assertEquals(0, changesIdxNameSub1Added.removedIndices.size)
        assertEquals(setOf(idxNameSub1), changesIdxNameSub1Added.addedIndices)

        val idxNameMaster = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = master.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        val changesIdxMasterAdded = indexTree.addIndex(idxNameMaster)
        assertEquals(0, changesIdxMasterAdded.updatedIndices.size)
        assertEquals(0, changesIdxMasterAdded.removedIndices.size)
        assertEquals(
            setOf(
                "master:name.0",
                "Sub2:name.${sub2.branchingTimestamp}"
            ).sorted(),
            changesIdxMasterAdded.addedIndices.map { it.fingerprint }.sorted()
        )
    }

    @Test
    fun inheritanceIsBlockedByExistingIndices2() {
        val master = DummyBranch.MASTER
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = master.now - 100000L,
            now = master.now
        )
        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = sub1.branchingTimestamp + 3000L,
            now = master.now
        )
        val sub1b = sub1.createChild(
            name = "Sub1b",
            branchingTimestamp = sub1.branchingTimestamp + 4000L,
            now = master.now
        )
        val sub2 = master.createChild(
            name = "Sub2",
            branchingTimestamp = master.now - 50000L,
            now = master.now
        )
        val indexTree = this.createIndexTreeFromBranches(master, sub1, sub1a, sub1b, sub2)
        val idxNameSub1 = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.createRange(sub1.branchingTimestamp + 100, sub1a.branchingTimestamp - 1),
            branch = sub1.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        val changesIdxNameSub1Added = indexTree.addIndex(idxNameSub1)
        assertEquals(0, changesIdxNameSub1Added.updatedIndices.size)
        assertEquals(0, changesIdxNameSub1Added.removedIndices.size)
        assertEquals(setOf(idxNameSub1), changesIdxNameSub1Added.addedIndices)

        val idxNameMaster = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = master.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        val changesIdxMasterAdded = indexTree.addIndex(idxNameMaster)
        assertEquals(0, changesIdxMasterAdded.updatedIndices.size)
        assertEquals(0, changesIdxMasterAdded.removedIndices.size)
        assertEquals(
            setOf(
                "master:name.0",
                "Sub1:name.${sub1.branchingTimestamp}",
                "Sub2:name.${sub2.branchingTimestamp}"
            ).sorted(),
            changesIdxMasterAdded.addedIndices.map { it.fingerprint }.sorted()
        )
    }

    @Test
    fun canAddBranchToParentBranchWithExistingIndex() {
        val master = DummyBranch.MASTER
        val allBranches = mutableSetOf(master)
        val branchByName = allBranches.associateBy { it.name }.toMutableMap()
        val childBranchesDirect = allBranches.asSequence().map { parent ->
            val children = allBranches.asSequence().filter { it.origin == parent }.toMutableSet()
            parent to children
        }.toMap().toMutableMap()
        val childBranchesIndirect = childBranchesDirect.inferIndirect().toMutableMap()
        val indexTree = IndexTreeImpl(
            existingIndices = emptySet(),
            getBranchByName = branchByName::get
        ) { parent, recursive ->
            val resultSet = if (recursive) {
                childBranchesIndirect[parent]
                    ?: emptySet()
            } else {
                childBranchesDirect[parent]
                    ?: emptySet()
            }
            resultSet.toList()
        }

        val idxNameMaster = SecondaryIndexImpl(
            id = UUID.randomUUID().toString(),
            name = "name",
            indexer = NamedPayloadNameIndexer(),
            validPeriod = Period.eternal(),
            branch = master.name,
            parentIndexId = null,
            dirty = false,
            options = emptySet()
        )
        val changesIdxMasterAdded = indexTree.addIndex(idxNameMaster)
        assertEquals(0, changesIdxMasterAdded.updatedIndices.size)
        assertEquals(0, changesIdxMasterAdded.removedIndices.size)
        assertEquals(setOf(idxNameMaster), changesIdxMasterAdded.addedIndices)

        // we add a branch to master
        val sub1 = master.createChild(
            name = "Sub1",
            branchingTimestamp = 1000000L,
            now = master.now
        )

        allBranches += sub1
        branchByName[sub1.name] = sub1
        childBranchesDirect.getValue(master).add(sub1)
        childBranchesIndirect.getValue(master).add(sub1)

        val indexChangesSub1Created = indexTree.onBranchCreated(sub1)
        assertEquals(0, indexChangesSub1Created.updatedIndices.size)
        assertEquals(0, indexChangesSub1Created.removedIndices.size)
        assertEquals(setOf(
            "Sub1:name.${sub1.branchingTimestamp}"
        ).sorted(),
            indexChangesSub1Created.addedIndices.map { it.fingerprint }.sorted()
        )

        val sub1a = sub1.createChild(
            name = "Sub1a",
            branchingTimestamp = 20000000L,
            now = master.now
        )

        allBranches += sub1a
        branchByName[sub1a.name] = sub1a
        childBranchesDirect[sub1] = mutableSetOf(sub1a)
        childBranchesIndirect[sub1] = mutableSetOf(sub1a)
        childBranchesIndirect.getValue(master).add(sub1a)

        val indexChangesSub1aCreated = indexTree.onBranchCreated(sub1a)
        assertEquals(0, indexChangesSub1aCreated.updatedIndices.size)
        assertEquals(0, indexChangesSub1aCreated.removedIndices.size)
        assertEquals(setOf(
            "Sub1a:name.${sub1a.branchingTimestamp}"
        ).sorted(),
            indexChangesSub1aCreated.addedIndices.map { it.fingerprint }.sorted()
        )
    }


    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private fun createIndexTreeFromBranches(vararg allBranches: DummyBranch): IndexTree {
        return this.createIndexTree(allBranches.asSequence().toSet(), emptySet())
    }

    private fun createIndexTree(allBranches: Set<DummyBranch>, allIndices: Set<SecondaryIndex>): IndexTree {
        val branchByName = allBranches.associateBy { it.name }
        val childBranchesDirect = allBranches.asSequence().map { parent ->
            val children = allBranches.asSequence().filter { it.origin == parent }.toSet()
            parent to children
        }.toMap()
        val childBranchesIndirect = childBranchesDirect.inferIndirect()
        return IndexTreeImpl(
            existingIndices = allIndices,
            getBranchByName = branchByName::get
        ) { parent, recursive ->
            val resultSet = if (recursive) {
                childBranchesIndirect[parent]
                    ?: emptySet()
            } else {
                childBranchesDirect[parent]
                    ?: emptySet()
            }
            resultSet.toList()
        }
    }

    // =================================================================================================================
    // TEST DUMMIES
    // =================================================================================================================

    private class DummyBranch(
        name: String,
        private val origin: Branch?,
        branchingTimestamp: Long,
        private val now: Long
    ) : Branch {

        companion object {

            val MASTER = DummyBranch(
                name = ChronoDBConstants.MASTER_BRANCH_IDENTIFIER,
                origin = null,
                branchingTimestamp = -1,
                now = 1628522098769L,
            )

        }

        private val metadata = DummyBranchMetadata(
            name = name,
            parentName = origin?.name,
            branchingTimestamp = branchingTimestamp
        )


        override fun getName(): String {
            return this.metadata.name
        }

        override fun getOrigin(): Branch? {
            return this.origin
        }

        override fun getBranchingTimestamp(): Long {
            return this.metadata.branchingTimestamp
        }

        override fun getOriginsRecursive(): MutableList<Branch> {
            return if (origin == null) {
                // we are the master branch; by definition, we return an empty list (see JavaDoc).
                Lists.newArrayList()
            } else {
                // we are not the master branch. Ask the origin to create the list for us
                val origins = getOrigin()!!.originsRecursive
                // ... and add our immediate parent to it.
                origins.add(getOrigin())
                origins
            }
        }

        override fun getNow(): Long {
            return this.now
        }

        override fun getDirectoryName(): String {
            return this.metadata.directoryName
        }

        override fun getMetadata(): IBranchMetadata {
            return this.metadata
        }

        fun createChild(name: String, branchingTimestamp: Long, now: Long): DummyBranch {
            require(this.branchingTimestamp < branchingTimestamp) {
                "Branching timestamp of child is greater than or equal to branching timestamp of parent!"
            }
            return DummyBranch(
                name = name,
                origin = this,
                branchingTimestamp = branchingTimestamp,
                now = now,
            )
        }

        override fun toString(): String {
            return "DummyBranch['${this.name}' branched from '${this.origin?.name}' at timestamp ${this.branchingTimestamp}]"
        }
    }

    private class DummyBranchMetadata(
        private val name: String,
        private val parentName: String?,
        private val branchingTimestamp: Long
    ) : IBranchMetadata {

        override fun getName(): String {
            return this.name
        }

        override fun getParentName(): String? {
            return this.parentName
        }

        override fun getBranchingTimestamp(): Long {
            return this.branchingTimestamp
        }

        override fun getDirectoryName(): String {
            return "dir/${this.name}"
        }

    }

    private fun Map<DummyBranch, Set<DummyBranch>>.inferIndirect(): Map<DummyBranch, MutableSet<DummyBranch>> {
        return this.entries.asSequence().map { (parent, directChildren) ->
            val toVisit = Stack<DummyBranch>()
            directChildren.forEach(toVisit::push)
            val resultSet = mutableSetOf<DummyBranch>()
            while (toVisit.isNotEmpty()) {
                val child = toVisit.pop()
                val grandChildren = this[child]
                    ?: emptySet()
                resultSet += grandChildren
                grandChildren.forEach(toVisit::push)
            }
            parent to resultSet
        }.toMap()
    }

    private val SecondaryIndex.fingerprint: String
        get() {
            return "$branch:$name.${validPeriod.lowerBound}"
        }
}


