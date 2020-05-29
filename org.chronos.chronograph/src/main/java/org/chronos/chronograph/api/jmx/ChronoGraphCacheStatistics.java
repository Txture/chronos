package org.chronos.chronograph.api.jmx;

import org.chronos.chronodb.internal.api.cache.ChronoDBCache;

public class ChronoGraphCacheStatistics implements ChronoGraphCacheStatisticsMBean {

    private static final ChronoGraphCacheStatistics INSTANCE = new ChronoGraphCacheStatistics();

    public static ChronoGraphCacheStatistics getInstance(){
        return INSTANCE;
    }

    private ChronoDBCache cacheInstance;

    public void setCache(ChronoDBCache cache){
        this.cacheInstance = cache;
    }

    public int getCacheSize(){
        ChronoDBCache cache = this.cacheInstance;
        if(cache == null){
            return 0;
        }
        return cache.size();
    }

    public long getHitCount(){
        ChronoDBCache cache = this.cacheInstance;
        if(cache == null){
            return 0;
        }
        return cache.getStatistics().getCacheHitCount();
    }

    public long getMissCount(){
        ChronoDBCache cache = this.cacheInstance;
        if(cache == null){
            return 0;
        }
        return cache.getStatistics().getCacheMissCount();
    }

    public long getRequestCount(){
        ChronoDBCache cache = this.cacheInstance;
        if(cache == null){
            return 0;
        }
        return cache.getStatistics().getRequestCount();
    }

    public double getHitRate(){
        ChronoDBCache cache = this.cacheInstance;
        if(cache == null){
            return 0;
        }
        return cache.getStatistics().getCacheHitRatio();
    }

}
