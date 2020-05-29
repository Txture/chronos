package org.chronos.chronograph.api.jmx;

public interface ChronoGraphCacheStatisticsMBean {

    public int getCacheSize();

    public long getHitCount();

    public long getMissCount();

    public long getRequestCount();

    public double getHitRate();

}
