package org.chronos.chronodb.exodus.builder

import org.chronos.chronodb.api.builder.database.ChronoDBBackendBuilder
import java.io.File

interface ExodusChronoDBBuilder : ChronoDBBackendBuilder {

    /**
     * Creates a new ChronoDB based on the Exodus backend, using the given directory as storage location.
     *
     * @param file The directory to use as storage location. Must not be `null`. Must exist, and must be a file (not a directory).
     * @return The database builder, for method chaining.
     */
    abstract fun onFile(file: File): ExodusChronoDBFinalizableBuilder

    /**
     * Creates a new ChronoDB based on the chunked backend, using the given file as storage location.
     *
     * @param filePath The file to use as storage location. Must not be `null`. Must point to an existing file (not a directory).
     * @return The database builder, for method chaining.
     */
    fun onFile(filePath: String): ExodusChronoDBFinalizableBuilder {
        return this.onFile(File(filePath))
    }
}