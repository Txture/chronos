package org.chronos.chronodb.api.dump;

import java.io.File;

import static com.google.common.base.Preconditions.*;

public class IncrementalBackupResult {

    private File cibFile;
    private IncrementalBackupInfo metadata;

    public IncrementalBackupResult(File cibFile, IncrementalBackupInfo metadata){
        checkNotNull(cibFile, "Precondition violation - argument 'cibFile' must not be NULL!");
        checkNotNull(metadata, "Precondition violation - argument 'metadata' must not be NULL!");
        this.cibFile = cibFile;
        this.metadata = metadata;
    }

    public File getCibFile() {
        return cibFile;
    }

    public IncrementalBackupInfo getMetadata() {
        return metadata;
    }
}
