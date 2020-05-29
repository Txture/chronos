package org.chronos.chronodb.exodus.layout

object ChronoDBDirectoryLayout {

    // DIRECTORIES
    const val GLOBAL_DIRECTORY = "global"
    const val BRANCHES_DIRECTORY = "branches"
    const val BRANCH_DIRECTORY_PREFIX = "branch_"
    const val CHUNK_DIRECTORY_PREFIX = "chunk"
    const val CHUNK_DATA_DIRECTORY = "data"
    const val CHUNK_INDEX_DIRCTORY = "index"
    const val CHUNK_METADATA_DIRECTORY = "meta"
    const val MASTER_BRANCH_DIRECTORY = BRANCH_DIRECTORY_PREFIX + "master"

    const val CHUNK_DIRECTORY_REGEX = "$CHUNK_DIRECTORY_PREFIX[0-9]+"

    // FILES
    const val CHUNK_INFO_PROPERTIES = "chunkinfo.properties"
    const val CHUNK_LOCK_FILE = "chunk.lck"
    const val BRANCH_INFO_PROPERTIES = "branchinfo.properties"

}