package org.chronos.chronograph.api.exceptions;

public class GraphTriggerScriptInstantiationException extends GraphTriggerScriptException {

    public GraphTriggerScriptInstantiationException() {
    }

    protected GraphTriggerScriptInstantiationException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GraphTriggerScriptInstantiationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GraphTriggerScriptInstantiationException(final String message) {
        super(message);
    }

    public GraphTriggerScriptInstantiationException(final Throwable cause) {
        super(cause);
    }

}
