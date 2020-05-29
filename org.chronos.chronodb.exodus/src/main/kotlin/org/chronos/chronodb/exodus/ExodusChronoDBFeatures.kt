package org.chronos.chronodb.exodus

import org.chronos.chronodb.api.ChronoDBFeatures

object ExodusChronoDBFeatures: ChronoDBFeatures {

    override fun isPersistent(): Boolean {
        return true
    }

    override fun isRolloverSupported(): Boolean {
        return true
    }

    override fun isIncrementalBackupSupported(): Boolean {
        return true
    }

}