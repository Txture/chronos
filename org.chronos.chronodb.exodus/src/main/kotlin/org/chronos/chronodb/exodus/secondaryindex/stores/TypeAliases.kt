package org.chronos.chronodb.exodus.secondaryindex.stores

import org.chronos.chronodb.internal.api.Period

typealias RawIndexEntryConsumer<V> = (storeName: String, primaryKey: String, indexedValue: V, validityPeriods: List<Period>)->Unit
typealias IndexEntryConsumer<V> = (IndexEntry<V>)->Unit

typealias IndexScanConsumer<V> = (primaryKey: String, indexedValue: V) -> Unit