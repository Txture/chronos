package org.chronos.chronograph.api.iterators.callbacks;

@FunctionalInterface
public interface TimestampChangeCallback {

    public static final TimestampChangeCallback IGNORE = (previousTimestamp, newTimestamp) -> {
    };


    public void handleTimestampChange(Long previousTimestamp, Long newTimestamp);


}
