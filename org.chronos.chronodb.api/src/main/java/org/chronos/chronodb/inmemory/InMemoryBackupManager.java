package org.chronos.chronodb.inmemory;

import org.chronos.chronodb.api.dump.IncrementalBackupResult;
import org.chronos.chronodb.internal.impl.engines.base.AbstractBackupManager;

import java.io.File;
import java.util.List;

public class InMemoryBackupManager extends AbstractBackupManager {

    public InMemoryBackupManager(final InMemoryChronoDB db) {
        super(db);
    }

    @Override
    public IncrementalBackupResult createIncrementalBackup(final long minTimestamp, final long lastRequestWallClockTime) {
        throw new UnsupportedOperationException("The in-memory backend does not support incremental backups. Use full backups instead.");
    }

    @Override
    public void loadIncrementalBackups(final List<File> cibFiles) {
        throw new UnsupportedOperationException("The in-memory backend does not support incremental backups. Use full backups instead.");
    }
}
