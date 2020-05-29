package org.chronos.chronodb.exodus.secondaryindex

import org.chronos.chronodb.exodus.kotlin.ext.requireNonNegative
import java.util.*

class ExodusIndexModifications {

    val changeTimestamp: Long
    private val entryAdditions: MutableList<ExodusIndexEntryAddition> = mutableListOf()
    private val entryTerminations: MutableList<ExodusIndexEntryTermination> = mutableListOf()

    constructor(changeTimestamp: Long){
        requireNonNegative(changeTimestamp, "changeTimestamp")
        this.changeTimestamp = changeTimestamp
    }

    val additions: List<ExodusIndexEntryAddition>
        get() = Collections.unmodifiableList(this.entryAdditions)

    val terminations: List<ExodusIndexEntryTermination>
        get() = Collections.unmodifiableList(this.entryTerminations)

    fun addEntryAddition(addition: ExodusIndexEntryAddition) {
        this.entryAdditions.add(addition)
    }

    fun addEntryTermination(termination: ExodusIndexEntryTermination) {
        this.entryTerminations.add(termination)
    }

    fun groupByBranch(): Map<String, ExodusIndexModifications> {
        val branchToModifications = mutableMapOf<String, ExodusIndexModifications>()
        for(action in this.entryAdditions) {
            var branchMods = branchToModifications[action.branch]
            if (branchMods == null) {
                branchMods = ExodusIndexModifications(this.changeTimestamp)
                branchToModifications[action.branch] = branchMods
            }
            branchMods.addEntryAddition(action)
        }
        for(action in this.entryTerminations) {
            var branchMods = branchToModifications[action.branch]
            if (branchMods == null) {
                branchMods = ExodusIndexModifications(this.changeTimestamp)
                branchToModifications[action.branch] = branchMods
            }
            branchMods.addEntryTermination(action)
        }
        return branchToModifications
    }

    fun clear() {
        this.entryAdditions.clear()
        this.entryTerminations.clear()
    }

    override fun toString(): String {
        return "ExodusIndexModifications(changeTimestamp=$changeTimestamp, entryAdditions=$entryAdditions, entryTerminations=$entryTerminations)"
    }

    val isEmpty: Boolean
        get(){
            return this.additions.isEmpty() && this.terminations.isEmpty()
        }

    val isNotEmpty: Boolean
        get() = !this.isEmpty



}

data class ExodusIndexEntryAddition(
    val branch: String,
    val index: String,
    val keyspace: String,
    val key: String,
    val value: Any
)

data class ExodusIndexEntryTermination(
    val branch: String,
    val index: String,
    val keyspace: String,
    val key: String,
    val value: Any
)