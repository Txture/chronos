package org.chronos.chronograph.api.exceptions;

public class GraphTriggerClassNotFoundException extends GraphTriggerException {

    public GraphTriggerClassNotFoundException() {
    }

    protected GraphTriggerClassNotFoundException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GraphTriggerClassNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GraphTriggerClassNotFoundException(final String message) {
        super(message);
    }

    public GraphTriggerClassNotFoundException(final Throwable cause) {
        super(cause);
    }

}
