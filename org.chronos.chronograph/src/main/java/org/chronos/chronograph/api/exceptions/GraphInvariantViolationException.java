package org.chronos.chronograph.api.exceptions;

public class GraphInvariantViolationException extends ChronoGraphException {

    public GraphInvariantViolationException() {
    }

    protected GraphInvariantViolationException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GraphInvariantViolationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GraphInvariantViolationException(final String message) {
        super(message);
    }

    public GraphInvariantViolationException(final Throwable cause) {
        super(cause);
    }
}
