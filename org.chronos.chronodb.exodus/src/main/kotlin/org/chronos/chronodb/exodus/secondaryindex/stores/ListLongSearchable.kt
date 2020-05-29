package org.chronos.chronodb.exodus.secondaryindex.stores

class ListLongSearchable : LongSearchable {

    private val list: List<Long>

    constructor(list: List<Long>){
        this.list = list
    }

    override val size: Int
        get() = this.list.size

    override fun get(index: Int): Long {
        return this.list[index]
    }
}