package org.chronos.chronodb.api.exceptions;

public class ChronoDBBackupException extends ChronoDBException {
    public ChronoDBBackupException() {
    }

    protected ChronoDBBackupException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public ChronoDBBackupException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ChronoDBBackupException(final String message) {
        super(message);
    }

    public ChronoDBBackupException(final Throwable cause) {
        super(cause);
    }
}
