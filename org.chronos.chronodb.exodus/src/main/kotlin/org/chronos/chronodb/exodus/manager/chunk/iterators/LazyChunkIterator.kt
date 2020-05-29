package org.chronos.chronodb.exodus.manager.chunk.iterators

import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk

class LazyChunkIterator<T> : Iterator<T> {

    private val createElementIterator: (ChronoChunk, Boolean, Boolean) -> Iterator<T>
    private var currentChunkElementIterator: Iterator<T>? = null
    private val chunkIterator: Iterator<ChronoChunk>
    private var isFirst: Boolean = true

    constructor(chunkIterator: Iterator<ChronoChunk>, createElementIterator: (ChronoChunk, Boolean, Boolean) -> Iterator<T>) {
        this.chunkIterator = chunkIterator
        this.createElementIterator = createElementIterator
        this.moveToNextChunkIfExhausted()
    }

    private fun moveToNextChunkIfExhausted() {
        if (this.hasNext()) {
            // current chunk is not exhausted yet
            return
        }
        if (!this.chunkIterator.hasNext()) {
            // no new chunk to move to
            this.currentChunkElementIterator = null
            return
        }
        // fetch chunk
        this.currentChunkElementIterator = null
        while (this.currentChunkElementIterator == null && this.chunkIterator.hasNext()) {
            val chunk = this.chunkIterator.next()
            this.currentChunkElementIterator = this.createElementIterator(chunk, isFirst, !this.chunkIterator.hasNext())
            if (this.currentChunkElementIterator != null && !this.currentChunkElementIterator!!.hasNext()) {
                this.currentChunkElementIterator = null
            }
        }
    }

    override fun hasNext(): Boolean {
        val iterator = this.currentChunkElementIterator
        if(iterator == null){
            return false
        }
        return iterator.hasNext()
    }

    override fun next(): T {
        if (!this.hasNext()) {
            throw IllegalStateException("Iterator has no more elements!")
        }
        this.isFirst = false
        val element = this.currentChunkElementIterator!!.next()
        this.moveToNextChunkIfExhausted()
        return element
    }

}