package org.chronos.chronodb.exodus.manager

import com.google.common.collect.Maps
import org.chronos.chronodb.api.BranchHeadStatistics
import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.kotlin.ext.orIfNull
import org.chronos.chronodb.internal.impl.engines.base.AbstractStatisticsManager

class ExodusStatisticsManager : AbstractStatisticsManager {

    private val owningDB: ExodusChronoDB
    private val branchHeadStatisticsCache: MutableMap<String, BranchHeadStatistics> = Maps.newConcurrentMap()

    constructor(owningDB: ExodusChronoDB): super() {
        this.owningDB = owningDB
    }

    override fun loadBranchHeadStatistics(branch: String): BranchHeadStatistics {
        return this.branchHeadStatisticsCache[branch].orIfNull {
            val branchInternal = this.owningDB.branchManager.getBranch(branch)
            val tkvs = branchInternal.temporalKeyValueStore
            val statistics = tkvs.calculateBranchHeadStatistics()
            this.saveBranchHeadStatistics(branch, statistics)
            return@orIfNull statistics
        }
    }

    override fun saveBranchHeadStatistics(branchName: String, statistics: BranchHeadStatistics) {
        this.branchHeadStatisticsCache[branchName] = statistics
    }

    override fun deleteBranchHeadStatistics() {
        this.branchHeadStatisticsCache.clear()
    }

    override fun deleteBranchHeadStatistics(branchName: String) {
        this.branchHeadStatisticsCache.remove(branchName)
    }

}