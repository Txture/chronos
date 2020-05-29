package org.chronos.chronodb.exodus.transaction

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.env.Cursor

object EmptyCursor : Cursor {

    override fun getPrevNoDup(): Boolean {
        return false
    }

    override fun getLast(): Boolean {
        return false
    }

    override fun getPrev(): Boolean {
        return false
    }

    override fun getNext(): Boolean {
        return false
    }

    override fun getSearchKeyRange(key: ByteIterable): ByteIterable? {
        return null
    }

    override fun isMutable(): Boolean {
        return false
    }

    override fun getNextNoDup(): Boolean {
        return false
    }

    override fun getNextDup(): Boolean {
        return false
    }

    override fun getSearchBothRange(key: ByteIterable, value: ByteIterable): ByteIterable? {
        return null
    }

    override fun count(): Int {
        return 0
    }

    override fun getKey(): ByteIterable {
        throw NullPointerException("Cursor is at initial position outside the store; successful call to getNext() is required to move it into the store.")
    }

    override fun getSearchBoth(key: ByteIterable, value: ByteIterable): Boolean {
        return false
    }

    override fun getPrevDup(): Boolean {
        return false
    }

    override fun getValue(): ByteIterable {
        throw NullPointerException("Cursor is at initial position outside the store; successful call to getNext() is required to move it into the store.")
    }

    override fun getSearchKey(key: ByteIterable): ByteIterable? {
        return null
    }

    override fun deleteCurrent(): Boolean {
        return false
    }

    override fun close() {
    }

}