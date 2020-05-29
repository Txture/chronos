package org.chronos.chronograph.api.exceptions;

public class GraphTriggerScriptCompilationException extends GraphTriggerScriptException {

    public GraphTriggerScriptCompilationException() {
    }

    protected GraphTriggerScriptCompilationException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GraphTriggerScriptCompilationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public GraphTriggerScriptCompilationException(final String message) {
        super(message);
    }

    public GraphTriggerScriptCompilationException(final Throwable cause) {
        super(cause);
    }

}
