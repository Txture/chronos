package org.chronos.chronodb.exodus.builder

import org.chronos.chronodb.exodus.ExodusChronoDB
import org.chronos.chronodb.exodus.configuration.ExodusChronoDBConfiguration
import org.chronos.chronodb.internal.api.ChronoDBConfiguration
import org.chronos.chronodb.internal.impl.builder.database.AbstractChronoDBFinalizableBuilder
import java.io.File

class ExodusChronoDBBuilderImpl : AbstractChronoDBFinalizableBuilder<ExodusChronoDBFinalizableBuilder>(), ExodusChronoDBBuilder, ExodusChronoDBFinalizableBuilder {

    override fun onFile(file: File): ExodusChronoDBFinalizableBuilder {
		require(file.isDirectory) {  "Precondition violation - argument 'file' must be a directory (not a file): ${file.absolutePath}" }
		require(file.exists()) { "Precondition violation - argument 'directory' must exist, but does not! Searched here: '${file.absolutePath}'" }
		this.withProperty(ChronoDBConfiguration.STORAGE_BACKEND, ExodusChronoDB.BACKEND_NAME)
		this.withProperty(ExodusChronoDBConfiguration.WORK_DIR, file.absolutePath)
		return this
    }

}