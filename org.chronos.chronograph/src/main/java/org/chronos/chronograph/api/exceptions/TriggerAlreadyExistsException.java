package org.chronos.chronograph.api.exceptions;

public class TriggerAlreadyExistsException extends ChronoGraphException {

    public TriggerAlreadyExistsException() {
    }

    protected TriggerAlreadyExistsException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public TriggerAlreadyExistsException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public TriggerAlreadyExistsException(final String message) {
        super(message);
    }

    public TriggerAlreadyExistsException(final Throwable cause) {
        super(cause);
    }

}
