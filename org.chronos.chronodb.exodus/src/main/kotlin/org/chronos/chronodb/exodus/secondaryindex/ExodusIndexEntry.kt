package org.chronos.chronodb.exodus.secondaryindex

import org.chronos.chronodb.api.key.ChronoIdentifier

data class ExodusIndexEntry(
    val branch: String,
    val indexName: String,
    val keyspace: String,
    val key: String,
    val indexedValue: Any,
    val validFrom: Long,
    val validTo: Long
) {

    constructor(identifier: ChronoIdentifier, indexName: String, value: Any) :
        this(
            identifier.branchName,
            indexName,
            identifier.keyspace,
            identifier.key,
            value,
            identifier.timestamp,
            Long.MAX_VALUE
        )
}