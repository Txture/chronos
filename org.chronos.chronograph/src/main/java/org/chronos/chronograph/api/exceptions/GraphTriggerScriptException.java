package org.chronos.chronograph.api.exceptions;

public class GraphTriggerScriptException extends GraphTriggerException {

    public GraphTriggerScriptException() {
    }

    protected GraphTriggerScriptException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GraphTriggerScriptException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GraphTriggerScriptException(final String message) {
        super(message);
    }

    public GraphTriggerScriptException(final Throwable cause) {
        super(cause);
    }

}
