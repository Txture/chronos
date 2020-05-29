package org.chronos.chronodb.exodus.secondaryindex.stores

interface LongSearchable {

    operator fun get(index: Int): Long

    val size: Int

    fun isEmpty(): Boolean {
        return this.size <= 0
    }

}