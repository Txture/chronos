package org.chronos.chronodb.api

enum class TextCompare {

    STRICT {

        override fun <T : Comparable<*>> apply(value: T): T {
            return value
        }

    },

    CASE_INSENSITIVE {

        @Suppress("UNCHECKED_CAST")
        override fun <T : Comparable<*>> apply(value: T): T {
            if(value is String){
                return value.lowercase() as T
            }else{
                return value
            }
        }

    };

    companion object {

        @JvmField
        val DEFAULT = STRICT

    }

    abstract fun <T: Comparable<*>> apply(value: T): T

}