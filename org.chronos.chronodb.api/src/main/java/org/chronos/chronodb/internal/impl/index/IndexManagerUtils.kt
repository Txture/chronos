package org.chronos.chronodb.internal.impl.index

object IndexManagerUtils {

    @JvmStatic
    @Suppress("unchecked_cast")
    fun Iterable<*>.smallestComparable(): Comparable<*> {
        var result: Comparable<*>? = null
        for(element in this){
            if(element is Comparable<*>){
                if(result == null || (element as Comparable<Any>) < result){
                    result = element
                }
            }
        }
        if(result == null){
            throw IllegalArgumentException("The given iterable had no smallest comparable! Size: ${this.count()}")
        }
        return result
    }

    @JvmStatic
    @Suppress("unchecked_cast")
    fun Sequence<*>.smallestComparable(): Comparable<*> {
        var result: Comparable<*>? = null
        for(element in this){
            if(element is Comparable<*>){
                if(result == null || (element as Comparable<Any>) < result){
                    result = element
                }
            }
        }
        if(result == null){
            throw IllegalArgumentException("The given iterable had no smallest comparable! Size: ${this.count()}")
        }
        return result
    }

    @JvmStatic
    @Suppress("unchecked_cast")
    fun Iterable<*>.largestComparable(): Comparable<*> {
        var result: Comparable<*>? = null
        for(element in this){
            if(element is Comparable<*>){
                if(result == null || (element as Comparable<Any>) > result){
                    result = element
                }
            }
        }
        if(result == null){
            throw IllegalArgumentException("The given iterable had no largest comparable! Size: ${this.count()}")
        }
        return result
    }

    @JvmStatic
    @Suppress("unchecked_cast")
    fun Sequence<*>.largestComparable(): Comparable<*> {
        var result: Comparable<*>? = null
        for(element in this){
            if(element is Comparable<*>){
                if(result == null || (element as Comparable<Any>) > result){
                    result = element
                }
            }
        }
        if(result == null){
            throw IllegalArgumentException("The given iterable had no largest comparable! Size: ${this.count()}")
        }
        return result
    }

    @JvmStatic
    fun Map<String, Set<Any>>.reduceValuesToSmallestComparable(): Map<String, Comparable<*>> {
        return this.asSequence().map { (key, values) ->
            key to values.smallestComparable()
        }.toMap()
    }

    @JvmStatic
    fun Map<String, Set<Any>>.reduceValuesToLargestComparable(): Map<String, Comparable<*>> {
        return this.asSequence().map { (key, values) ->
            if(values.size <= 1){
                key to values
            }
            key to values.largestComparable()
        }.toMap()
    }

}