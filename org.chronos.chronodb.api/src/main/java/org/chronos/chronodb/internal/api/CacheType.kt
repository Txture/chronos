package org.chronos.chronodb.internal.api

enum class CacheType {
    MOSAIC,
    HEAD_FIRST;

    companion object {
        @JvmStatic
        fun fromString(text: String?): CacheType {
            return when (text?.lowercase()?.trim()) {
                "mosaic", null -> MOSAIC
                "headfirst", "head-first", "head_first" -> HEAD_FIRST
                else -> MOSAIC
            }
        }
    }
}