package org.chronos.chronodb.exodus.manager.chunk

import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.chronos.chronodb.exodus.kotlin.ext.*
import org.chronos.chronodb.internal.api.Period
import org.chronos.common.version.ChronosVersion
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import java.util.*

const val PROPERTY_VALID_FROM = "validFrom"
const val PROPERTY_VALID_TO = "validTo"
const val PROPERTY_BRANCH_NAME = "branch"
const val PROPERTY_SEQUENCE_NUMBER = "sequenceNumber"

data class ChunkMetadata(
        var validFrom: Long,
        var validTo: Long,
        val branchName: String,
        val sequenceNumber: Long) {

    companion object {

        @JvmStatic
        fun readFrom(directory: File): ChunkMetadata {
            requireExistingDirectory(directory, "directory")
            return Environments.newInstance(directory).use { env ->
                env.computeInReadonlyTransaction { tx ->
                    if (!env.storeExists("metadata", tx)) {
                        throw IllegalStateException("Chunk metadata is empty in: ${directory.absolutePath}")
                    }
                    val store = env.openStore("metadata", StoreConfig.WITHOUT_DUPLICATES, tx)
                    val validFrom = getLongFromStore(tx, store, PROPERTY_VALID_FROM)
                    val validTo = getLongFromStore(tx, store, PROPERTY_VALID_TO)
                    val sequenceNumber = getLongFromStore(tx, store, PROPERTY_SEQUENCE_NUMBER)
                    val branchName = getStringFromStore(tx, store, PROPERTY_BRANCH_NAME)
                    ChunkMetadata(
                            validFrom = validFrom,
                            validTo = validTo,
                            sequenceNumber = sequenceNumber,
                            branchName = branchName
                    )
                }
            }
        }

    }

    val validPeriod: Period
        get() = Period.createRange(this.validFrom, this.validTo)

    fun writeBinaryTo(directory: File) {
        requireDirectory(directory, "directory")
        Environments.newInstance(directory).use { env ->
            env.executeInExclusiveTransaction { tx ->
                if(env.storeExists("metadata", tx)){
                    env.truncateStore("metadata", tx)
                }
                val store = env.openStore("metadata", StoreConfig.WITHOUT_DUPLICATES, tx)
                store.put(tx, PROPERTY_VALID_FROM, this.validFrom.toByteIterable())
                store.put(tx, PROPERTY_VALID_TO, this.validTo.toByteIterable())
                store.put(tx, PROPERTY_BRANCH_NAME, this.branchName.toByteIterable())
                store.put(tx, PROPERTY_SEQUENCE_NUMBER, this.sequenceNumber.toByteIterable())
                tx.commit()
            }
        }
    }

    fun writePlainTextTo(chunkInfoFile: File) {
        requireFile(chunkInfoFile, "chunkInfoFile")
        val parentDir = chunkInfoFile.parentFile
        if(!parentDir.exists()){
            throw IllegalStateException("The parent directory of the given file does not exist: ${chunkInfoFile.absolutePath}")
        }
        // first, create the actual content in a temporary file
        val tempFile = File(parentDir, chunkInfoFile.name + ".tmp")
        val tempFilePath = tempFile.toPath()
        Files.deleteIfExists(tempFilePath)
        Files.createFile(tempFilePath)
        val properties = Properties()
        properties.setProperty(PROPERTY_BRANCH_NAME, this.branchName)
        properties.setProperty(PROPERTY_SEQUENCE_NUMBER, this.sequenceNumber.toString())
        properties.setProperty(PROPERTY_VALID_FROM, this.validFrom.toString())
        properties.setProperty(PROPERTY_VALID_TO, this.validTo.toString())
        BufferedWriter(FileWriter(tempFile)).use { writer ->
            writer.write("# CHUNK INFORMATION written by Chronos ${ChronosVersion.getCurrentVersion()}\n")
            writer.write("# This file is intended as a human-readable information source.\n")
            writer.write("# The contents of this file will be overwritten when needed. DO NOT MODIFY IT.\n")
            writer.write("# Last modification date: ${Date()}\n")
            writer.write("# ============================================================================\n")
            writer.write("\n")
            properties.store(writer, null)
            writer.flush()
        }
        // delete the previous chunk info file (if any)
        val chunkFilePath = chunkInfoFile.toPath()
        Files.deleteIfExists(chunkFilePath)
        // copy the temp file to the target position
        Files.copy(tempFilePath, chunkFilePath)
        // delete the temp file
        Files.delete(tempFilePath)
    }

}

private fun getLongFromStore(tx: Transaction, store: Store, key: String): Long {
    val value = store.get(tx, key)
    if (value == null) {
        throw IllegalStateException("Missing key: '${key}'!")
    }
    return value.parseAsLong()
}

private fun getStringFromStore(tx: Transaction, store: Store, key: String): String {
    val value = store.get(tx, key)
    if (value == null) {
        throw IllegalStateException("Missing key: '${key}'!")
    }
    return value.parseAsString()
}

