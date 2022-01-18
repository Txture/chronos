package org.chronos.chronodb.exodus.layout

object ChronoDBStoreLayout {

    const val STORE_NAME__BRANCH_METADATA = "chronos.management.branches"
    const val STORE_NAME__COMMIT_METADATA = "chronos.management.commitMetadata"
    const val STORE_NAME__NAVIGATION = "chronos.management.navigation"
    const val STORE_NAME__BRANCH_TO_NOW = "chronos.management.branchToNow"
    const val STORE_NAME__BRANCH_TO_WAL = "chronos.management.branchToWAL"
    const val STORE_NAME__CHRONOS_VERSION = "chronos.management.chronosVersion"
    const val STORE_NAME__INDEXERS = "chronos.management.indexers"
    const val STORE_NAME__INDEXDIRTY = "chronos.management.indexdirty"
    const val STORE_NAME__DATEBACK_LOG = "chronos.management.datebacklog"

    const val STORE_NAME_PREFIX__MATRIX = "matrix_"
    const val STORE_NAME_PREFIX__SECONDARY_INDEX_STRING = "secondaryIndex_String_"
    const val STORE_NAME_PREFIX__SECONDARY_INDEX_STRING_CASE_INSENSITIVE = "secondaryIndex_StringCI_"
    const val STORE_NAME_PREFIX__SECONDARY_INDEX_DOUBLE = "secondaryIndex_Double_"
    const val STORE_NAME_PREFIX__SECONDARY_INDEX_LONG = "secondaryIndex_Long_"

    const val KEY__CHRONOS_VERSION = "chronos.version"
    @Deprecated("indexers are not stored under a single key anymore, but by their ID instead.")
    const val KEY__ALL_INDEXERS = "chronos.indexers"


}