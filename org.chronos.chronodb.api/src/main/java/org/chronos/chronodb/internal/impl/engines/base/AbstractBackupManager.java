package org.chronos.chronodb.internal.impl.engines.base;

import com.google.common.io.Files;
import org.chronos.chronodb.api.BackupManager;
import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.DumpOption;
import org.chronos.chronodb.api.dump.ChronoDBDumpFormat;
import org.chronos.chronodb.api.dump.IncrementalBackupInfo;
import org.chronos.chronodb.api.dump.IncrementalBackupResult;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronodb.internal.api.ChronoDBInternal;
import org.chronos.chronodb.internal.api.stream.ObjectInput;
import org.chronos.chronodb.internal.api.stream.ObjectOutput;
import org.chronos.chronodb.internal.impl.dump.ChronoDBDumpUtil;
import org.chronos.chronodb.internal.impl.dump.DumpOptions;
import org.chronos.common.autolock.AutoLock;

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractBackupManager implements BackupManager {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    private final ChronoDBInternal owningDB;

    // =================================================================================================================
    // CONSTRUCTOR
    // =================================================================================================================

    public AbstractBackupManager(ChronoDBInternal db){
        checkNotNull(db, "Precondition violation - argument 'db' must not be NULL!");
        this.owningDB = db;
    }

    // =================================================================================================================
    // PUBLIC API
    // =================================================================================================================

    @Override
    public void writeDump(final File dumpFile, final DumpOption... dumpOptions) {
        checkNotNull(dumpFile, "Precondition violation - argument 'dumpFile' must not be NULL!");
        if (dumpFile.exists()) {
            checkArgument(dumpFile.isFile(),
                "Precondition violation - argument 'dumpFile' must be a file (not a directory)!");
        } else {
            try {
                File dumpParentDir = dumpFile.getParentFile();
                if (!dumpParentDir.exists()) {
                    boolean dirsCreated = dumpFile.getParentFile().mkdirs();
                    if (!dirsCreated) {
                        throw new IOException("Failed to create directory path '" + dumpParentDir.getAbsolutePath() + "'! Please check access permissions.");
                    }
                }
                boolean createdDumpFile = dumpFile.createNewFile();
                if (!createdDumpFile) {
                    throw new IOException("Failed to create file '" + dumpFile.getAbsolutePath() + "'! Please check access permissions.");
                }
            } catch (IOException e) {
                throw new ChronoDBStorageBackendException(
                    "Failed to create dump file in '" + dumpFile.getAbsolutePath() + "'!", e);
            }
        }
        DumpOptions options = new DumpOptions(dumpOptions);
        try (AutoLock lock = this.owningDB.lockNonExclusive()) {
            try (ObjectOutput output = ChronoDBDumpFormat.createOutput(dumpFile, options)) {
                ChronoDBDumpUtil.dumpDBContentsToOutput(this.owningDB, output, options);
            }
        }
    }

    @Override
    public void readDump(final File dumpFile, final DumpOption... dumpOptions) {
        checkNotNull(dumpFile, "Precondition violation - argument 'dumpFile' must not be NULL!");
        checkArgument(dumpFile.exists(),
            "Precondition violation - argument 'dumpFile' does not exist! Location: " + dumpFile.getAbsolutePath());
        checkArgument(dumpFile.isFile(),
            "Precondition violation - argument 'dumpFile' must be a File (is a Directory)!");
        this.owningDB.getConfiguration().assertNotReadOnly();
        DumpOptions options = new DumpOptions(dumpOptions);
        try (AutoLock lock = this.owningDB.lockExclusive()) {
            try (ObjectInput input = ChronoDBDumpFormat.createInput(dumpFile, options)) {
                ChronoDBDumpUtil.readDumpContentsFromInput(this.owningDB, input, options);
            }
        }
    }

    // =================================================================================================================
    // INTERNAL API
    // =================================================================================================================

    protected ChronoDBInternal getOwningDb(){
        return this.owningDB;
    }

}
