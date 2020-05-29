package org.chronos.chronograph.api.transaction.trigger;

public class CancelCommitException extends RuntimeException {

    public CancelCommitException() {
    }

    public CancelCommitException(final String message) {
        super(message);
    }

    public CancelCommitException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public CancelCommitException(final Throwable cause) {
        super(cause);
    }

    protected CancelCommitException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
