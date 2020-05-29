package org.chronos.chronodb.api.exceptions;

public class ChronoDBCommitMetadataRejectedException extends ChronoDBCommitException {

    public ChronoDBCommitMetadataRejectedException() {
    }

    protected ChronoDBCommitMetadataRejectedException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public ChronoDBCommitMetadataRejectedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ChronoDBCommitMetadataRejectedException(final String message) {
        super(message);
    }

    public ChronoDBCommitMetadataRejectedException(final Throwable cause) {
        super(cause);
    }

}
