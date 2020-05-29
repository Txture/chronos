package org.chronos.chronodb.exodus.manager.chunk.iterators

import org.chronos.chronodb.exodus.manager.chunk.ChronoChunk
import org.chronos.chronodb.exodus.manager.chunk.GlobalChunkManager
import org.chronos.chronodb.internal.api.stream.CloseableIterator
import org.chronos.chronodb.internal.impl.stream.AbstractCloseableIterator
import org.chronos.chronodb.internal.impl.temporal.UnqualifiedTemporalEntry
import java.util.NoSuchElementException

class AllEntriesIterator : AbstractCloseableIterator<UnqualifiedTemporalEntry> {

    private val storeName: String
    private val maxTimestamp: Long
    private val minTimestamp: Long
    private val globalChunkManager: GlobalChunkManager

    private val innerIterator: LazyChunkIterator<UnqualifiedTemporalEntry>
    private var currentIterator: CloseableIterator<UnqualifiedTemporalEntry>? = null
    private val includeRolloverCommits: Boolean


    constructor(globalChunkManager: GlobalChunkManager, chunks: List<ChronoChunk>, storeName: String, minTimestamp: Long, maxTimestamp: Long, includeRolloverCommits: Boolean) {
        require(minTimestamp >= 0) { "Precondition violation - argument 'minTimestamp' must be greater than or equal to zero!" }
        require(maxTimestamp >= 0) { "Precondition violation - argument 'maxTimestamp' must be greater than or equal to zero!" }
        require(minTimestamp <= maxTimestamp) { "Precondition violation - argument 'minTimestamp' must be less than or equal to 'maxTimestamp'!" }
        this.globalChunkManager = globalChunkManager
        this.minTimestamp = minTimestamp
        this.maxTimestamp = maxTimestamp
        this.storeName = storeName
        this.includeRolloverCommits = includeRolloverCommits
        this.innerIterator = LazyChunkIterator(chunks.iterator(), this::createChunkElementIterator)
    }

    override fun next(): UnqualifiedTemporalEntry {
        if (!this.hasNext()) {
            throw NoSuchElementException("Iterator is exhausted!")
        }
        return this.innerIterator.next()
    }

    override fun hasNextInternal(): Boolean {
        return this.innerIterator.hasNext()
    }

    override fun closeInternal() {
        if (this.currentIterator != null) {
            this.currentIterator!!.close()
            this.currentIterator = null
        }
    }

    protected fun createChunkElementIterator(chunk: ChronoChunk, isFirst: Boolean, isLast: Boolean): Iterator<UnqualifiedTemporalEntry> {
        if (this.currentIterator != null) {
            this.currentIterator!!.close()
        }
        val tx = this.globalChunkManager.openReadOnlyTransactionOn(chunk)
        // all entries iterators are always in ASCENDING chunk order. Therefore, at the first chunk ( = origin chunk )
        // we always want to INCLUDE the initial commits. At all other chunks, we rely on the setting passed to us.
        val includeRollovers = isFirst || this.includeRolloverCommits
        this.currentIterator = AllChunkEntriesIterator(tx, storeName, minTimestamp, maxTimestamp, includeRollovers)
        return this.currentIterator!!.asIterator()
    }

}