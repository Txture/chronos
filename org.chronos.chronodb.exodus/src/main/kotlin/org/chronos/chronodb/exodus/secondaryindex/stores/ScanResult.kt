package org.chronos.chronodb.exodus.secondaryindex.stores

import org.chronos.chronodb.api.Order

data class ScanResult<T>(
    val entries: List<ScanResultEntry<T>>,
    val orderedBy: OrderedBy? = null
) : List<ScanResultEntry<T>> by entries {

    val isOrdered
        get() = this.orderedBy != null
}

data class OrderedBy(
    val propertyName: String,
    val order: Order
)