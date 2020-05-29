package org.chronos.chronodb.inmemory;

import org.chronos.chronodb.api.ChronoDBFeatures;

public class InMemoryFeatures implements ChronoDBFeatures {

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public boolean isRolloverSupported() {
        return false;
    }

    @Override
    public boolean isIncrementalBackupSupported() {
        return false;
    }

}
