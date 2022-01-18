package org.chronos.chronodb.internal.impl.index

interface IndexingOption {

    companion object {

        @JvmStatic
        fun assumeNoPriorValues(): IndexingOption {
            return StandardIndexingOption.ASSUME_NO_PRIOR_VALUES
        }

    }


    val inheritable: Boolean

    fun copy(): IndexingOption

}