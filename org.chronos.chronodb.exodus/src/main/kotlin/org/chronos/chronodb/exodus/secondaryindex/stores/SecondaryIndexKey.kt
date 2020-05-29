package org.chronos.chronodb.exodus.secondaryindex.stores

import jetbrains.exodus.ByteIterable
import org.chronos.chronodb.exodus.kotlin.ext.parseAsString

class SecondaryIndexKey<T> {

    val indexValueBinary: ByteIterable
    val primaryKeyBinary: ByteIterable

    private val indexValueParser: (ByteIterable) -> T

    val indexValuePlain: T by lazy { this.parseIndexValue() }
    val primaryKeyPlain: String by lazy { this.parsePrimaryKey() }

    constructor(indexValue: ByteIterable, primaryKey: ByteIterable, indexValueParser: (ByteIterable) -> T){
        this.indexValueBinary = indexValue
        this.primaryKeyBinary = primaryKey
        this.indexValueParser = indexValueParser
    }

    private fun parseIndexValue(): T {
        return this.indexValueParser(this.indexValueBinary)
    }

    private fun parsePrimaryKey(): String {
        return this.primaryKeyBinary.parseAsString()
    }

    fun toScanResultEntry(): ScanResultEntry<T> {
        return ScanResultEntry(this.indexValuePlain, this.primaryKeyPlain)
    }

    override fun toString(): String {
        return "SecondaryIndexKey[${this.indexValuePlain}|${this.primaryKeyPlain}]"
    }
}