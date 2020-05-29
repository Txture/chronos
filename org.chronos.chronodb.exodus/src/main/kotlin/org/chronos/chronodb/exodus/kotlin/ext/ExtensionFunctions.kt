package org.chronos.chronodb.exodus.kotlin.ext

import java.io.File
import java.nio.file.Files
import kotlin.reflect.KClass

fun <T, U: T> T?.orIfNull(other: U): T{
    if(this == null){
        return other
    }else{
        return this
    }
}

fun <T, U: T> T?.orIfNull(supplier: ()->U): T{
    if(this == null){
        return supplier()
    }else{
        return this
    }
}

inline fun <T, U> T?.mapSingle(transform: (T) -> U): U?{
    if(this == null){
        return null
    }else{
        return transform(this)
    }
}

fun <T> Iterator<T>.nextOrElse(default: T): T {
    if(this.hasNext()){
        return this.next()
    }else{
        return default
    }
}

fun <T> Iterable<T>.onlyElement(default: T? = null): T {
    val iterator = this.iterator()
    if(!iterator.hasNext()){
        if(default != null){
            return default
        }else{
            throw NoSuchElementException("Iterable is empty!")
        }
    }
    val onlyElement = iterator.next()
    if(iterator.hasNext()){
        throw IllegalStateException("Iterable has more than one element!")
    }
    return onlyElement
}


fun <T, U: Any> Sequence<T>.cast(clazz: KClass<U>): Sequence<U> {
    return this.cast(clazz.java)
}

fun <T, U: Any> Sequence<T>.cast(clazz: Class<U>): Sequence<U> {
    return this.filter(clazz::isInstance).map(clazz::cast)
}

fun <E: Any, K: Any, V: Any> Sequence<E>.mapTo(destination: MutableMap<K, V>, transform: (E)->Pair<K, V>) {
    this.forEach {
        val entry = transform(it)
        destination[entry.first] = entry.second
    }
}

fun <E: Any, K: Any, V: Any> Sequence<E>.toMap(transform: (E)->Pair<K, V>){
    this.map(transform).toMap()
}

fun <T> Iterable<T>.isNotEmpty(): Boolean{
    return !this.isEmpty()
}

fun <T> Iterable<T>.isEmpty(): Boolean{
    if(this is Collection){
        return this.isEmpty()
    }
    return !this.iterator().hasNext()
}


fun File.createDirectory() {
    Files.createDirectory(this.toPath())
}

fun File.createDirectoryIfNotExists(){
    val path = this.toPath()
    if(!Files.exists(path)){
        Files.createDirectory(path)
    }else{
        require(this.isDirectory){
            "Failed to create directory (a file with the same path already exists): ${this.absolutePath}"
        }
    }
}

fun File.createFile(){
    Files.createFile(this.toPath())
}

fun File.createFileIfNotExists(){
    val path = this.toPath()
    if(!Files.exists(path)){
        Files.createFile(path)
    }else{
        require(this.isFile){
            "Failed to create file (a directory with the same path already exists): ${this.absolutePath}"
        }
    }
}