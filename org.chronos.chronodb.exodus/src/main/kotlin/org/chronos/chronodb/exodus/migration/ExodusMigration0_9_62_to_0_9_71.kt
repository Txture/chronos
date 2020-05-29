package org.chronos.chronodb.exodus.migration

import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.internal.api.migration.ChronosMigration
import org.chronos.chronodb.internal.api.migration.annotations.Migration

@Migration(from = "0.9.62", to = "0.9.71")
class ExodusMigration0_9_62_to_0_9_71 : ChronosMigration<ExodusChronoDB> {

    override fun execute(chronoDB: ExodusChronoDB) {
        // Exodus ChronoDB had an issue with secondary indices being incorrect,
        // which was fixed in 0.9.70. In order for this fix to take effect, we
        // need to perform a reindexing on all branches.
        chronoDB.indexManager.reindexAll(true)
    }

}