package org.chronos.chronodb.exodus.secondaryindex.stores

data class ScanResultEntry<T>(
    val indexedValue: T,
    val primaryKey: String
)