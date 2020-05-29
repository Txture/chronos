package org.chronos.chronodb.api.exceptions;

public class DatebackException extends ChronoDBException {

	public DatebackException() {
		super();
	}

	protected DatebackException(final String message, final Throwable cause, final boolean enableSuppression,
			final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public DatebackException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public DatebackException(final String message) {
		super(message);
	}

	public DatebackException(final Throwable cause) {
		super(cause);
	}

}
