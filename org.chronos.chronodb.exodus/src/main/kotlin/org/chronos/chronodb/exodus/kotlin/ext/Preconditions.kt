package org.chronos.chronodb.exodus.kotlin.ext

import java.io.File

fun requireDirectory(file: File, varName: String): File {
    require(file.isDirectory) { "Precondition violation - argument '${varName}' must be a directory (not a file)! Path is: ${file.absolutePath}" }
    return file
}

fun requireFile(file: File, varName: String) : File{
    require(file.isFile) { "Precondition violation - argument '${varName}' must be a file (not a directory)! Path is: ${file.absolutePath}" }
    return file
}

fun requireExistingFile(file: File, varName: String) : File{
    requireFile(file, varName)
    require(file.exists()) { "Precondition violation - the file given by argument '${varName}' must exist! Path is: ${file.absolutePath}" }
    return file
}

fun requireExistingDirectory(file: File, varName: String) : File{
    requireDirectory(file, varName)
    require(file.exists()) { "Precondition violation - the directory given by argument '${varName}' must exist! Path is: ${file.absolutePath}" }
    return file
}

fun requireNonNegative(value: Long, varName: String) {
    require(value >= 0) { "Precondition violation - argument '${varName}' must not be negative! Value is: '${value}'" }
}


fun requireNonNegative(value: Int, varName: String) {
    require(value >= 0) { "Precondition violation - argument '${varName}' must not be negative! Value is: '${value}'" }
}