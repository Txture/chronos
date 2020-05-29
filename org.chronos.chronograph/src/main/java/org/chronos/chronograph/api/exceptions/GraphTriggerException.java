package org.chronos.chronograph.api.exceptions;

public class GraphTriggerException extends ChronoGraphException {

    public GraphTriggerException() {
    }

    protected GraphTriggerException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GraphTriggerException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GraphTriggerException(final String message) {
        super(message);
    }

    public GraphTriggerException(final Throwable cause) {
        super(cause);
    }

}
