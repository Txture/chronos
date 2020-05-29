package org.chronos.chronodb.api;

import org.chronos.chronodb.api.dump.IncrementalBackupInfo;
import org.chronos.chronodb.api.dump.IncrementalBackupResult;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.*;

public interface BackupManager {

    // =================================================================================================================
    // FULL DUMP WRITING / LOADING
    // =================================================================================================================

    /**
     * Creates a database dump of the entire current database state.
     *
     * <p>
     * Please note that the database is not available for write operations while the dump process is running. Read operations will work concurrently as usual.
     *
     * <p>
     * <b>WARNING:</b> The file created by this process may be very large (several gigabytes), depending on the size of the database. It is the responsibility of the user of this API to ensure that enough disk space is available; this method does not perform any checks regarding disk space availability!
     *
     * <p>
     * <b>WARNING:</b> The given file will be <b>overwritten</b> without further notice!
     *
     * @param dumpFile    The file to store the dump into. Must not be <code>null</code>. Must point to a file (not a directory). The standard file extension <code>*.chronodump</code> is recommmended, but not required. If the file does not exist, the file (and any missing parent directory) will be created.
     * @param dumpOptions The options to use for this dump (optional). Please refer to the documentation of the invididual constants for details.
     */
    public void writeDump(File dumpFile, DumpOption... dumpOptions);

    /**
     * Reads the contents of the given dump file into this database.
     *
     * <p>
     * This is a management operation; it completely locks the database. No concurrent writes or reads will be permitted while this operation is being executed.
     *
     * <p>
     * <b>WARNING:</b> The current contents of the database will be <b>merged</b> with the contents of the dump! In case of conflicts, the data stored in the dump file will take precedence. It is <i>strongly recommended</i> to perform this operation only on an <b>empty</b> database!
     *
     * <p>
     * <b>WARNING:</b> As this is a management operation, there is no rollback or undo option!
     *
     * @param dumpFile The dump file to read the data from. Must not be <code>null</code>, must exist, and must point to a file (not a directory).
     * @param options  The options to use while reading (optional). May be empty.
     */
    public void readDump(File dumpFile, DumpOption... options);

    // =================================================================================================================
    // INCREMENTAL COMMIT METHODS
    // =================================================================================================================

    public IncrementalBackupResult createIncrementalBackup(long minTimestamp, long lastRequestWallClockTime);

    public void loadIncrementalBackups(List<File> cibFiles);

    public default void loadIncrementalBackupsFromDirectory(File directory){
        checkNotNull(directory, "Precondition violation - argument 'directory' must not be NULL!");
        if(!directory.isDirectory()){
            throw new IllegalArgumentException("Precondition violation - argument 'directory' must be an existing directory: " + directory.getAbsolutePath());
        }
        // we are sure that the path exists and it is a directory
        // look for all '.cib' files
        File[] files = directory.listFiles(f -> f.getName().endsWith(ChronoDBConstants.INCREMENTAL_BACKUP_FILE_ENDING));
        if(files == null || files.length <= 0){
            return;
        }
        List<File> cibFiles = Arrays.asList(files);
        loadIncrementalBackups(cibFiles);
    }

}
