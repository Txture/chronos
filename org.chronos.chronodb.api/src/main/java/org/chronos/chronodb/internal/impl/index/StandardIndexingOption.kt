package org.chronos.chronodb.internal.impl.index

enum class StandardIndexingOption : IndexingOption {

    /**
     * Starts the secondary index with the assumption that all
     * objects prior to the index introduction do not have the indexed property.
     *
     * This assumption allows for faster re-indexing, but can lead to wrong
     * query results if the assumption doesn't hold on the actual data.
     */
    ASSUME_NO_PRIOR_VALUES {

        override val inheritable: Boolean = true

    };

    override fun copy(): IndexingOption {
        // enum values are immutable and therefore don't need copying.
        return this
    }
}