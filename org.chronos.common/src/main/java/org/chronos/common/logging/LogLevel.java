package org.chronos.common.logging;

import static com.google.common.base.Preconditions.*;

public enum LogLevel {

    OFF(0), TRACE(1), DEBUG(2), INFO(3), WARN(4), ERROR(5);

    private final int level;

    private LogLevel(final int level) {
        checkArgument(level >= 0, "LogLevel can not be negative!");
        this.level = level;
    }

    public boolean isGreaterThan(final LogLevel other) {
        return this.level > other.level;
    }

    public boolean isLessThan(final LogLevel other) {
        return this.level < other.level;
    }

    public boolean isGreaterThanOrEqualTo(final LogLevel other) {
        return this.level >= other.level;
    }

    public boolean isLessThanOrEqualTo(final LogLevel other) {
        return this.level <= other.level;
    }

    public static LogLevel fromString(String value) {
        if (value == null) {
            return null;
        }
        switch (value.toLowerCase().trim()) {
            case "off":
                return OFF;
            case "trace":
                return TRACE;
            case "debug":
                return DEBUG;
            case "info":
                return INFO;
            case "warn":
                return WARN;
            case "error":
                return ERROR;
            default:
                return null;
        }
    }
}
