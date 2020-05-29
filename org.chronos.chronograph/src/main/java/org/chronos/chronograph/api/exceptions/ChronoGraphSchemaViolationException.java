package org.chronos.chronograph.api.exceptions;

public class ChronoGraphSchemaViolationException extends ChronoGraphException {

    public ChronoGraphSchemaViolationException() {
    }

    protected ChronoGraphSchemaViolationException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public ChronoGraphSchemaViolationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ChronoGraphSchemaViolationException(final String message) {
        super(message);
    }

    public ChronoGraphSchemaViolationException(final Throwable cause) {
        super(cause);
    }
}
