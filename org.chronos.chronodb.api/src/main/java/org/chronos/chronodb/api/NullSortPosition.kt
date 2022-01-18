package org.chronos.chronodb.api

enum class NullSortPosition {

    NULLS_FIRST {

        override fun reversed(): NullSortPosition {
            return NULLS_LAST
        }

    },
    NULLS_LAST {

        override fun reversed(): NullSortPosition {
            return NULLS_FIRST
        }

    };

    abstract fun reversed(): NullSortPosition

    companion object {

        @JvmField
        val DEFAULT = NULLS_LAST

    }

}