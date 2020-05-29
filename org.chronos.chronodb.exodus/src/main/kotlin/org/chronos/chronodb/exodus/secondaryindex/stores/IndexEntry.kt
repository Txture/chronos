package org.chronos.chronodb.exodus.secondaryindex.stores

import org.chronos.chronodb.internal.api.Period

class IndexEntry<V>(
    val branch: String,
    val chunkIndex: Long,
    val storeName: String,
    val primaryKey: String,
    val indexValue: V,
    val validityPeriods: List<Period>
){

    override fun toString(): String {
        return "IndexEntry[${branch}#${chunkIndex}->${storeName}::${primaryKey}=${indexValue} for ${validityPeriods}]"
    }

}