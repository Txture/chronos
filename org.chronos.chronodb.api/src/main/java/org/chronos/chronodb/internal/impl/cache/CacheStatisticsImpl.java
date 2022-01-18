package org.chronos.chronodb.internal.impl.cache;

import java.util.concurrent.atomic.AtomicLong;

import org.chronos.chronodb.internal.api.cache.ChronoDBCache.CacheStatistics;

public class CacheStatisticsImpl implements CacheStatistics {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private AtomicLong hitCount;
	private AtomicLong missCount;
	private AtomicLong evictedCount;
	private AtomicLong rollbackCount;
	private AtomicLong clearCount;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public CacheStatisticsImpl() {
		this.hitCount = new AtomicLong(0L);
		this.missCount = new AtomicLong(0L);
		this.evictedCount = new AtomicLong(0L);
		this.rollbackCount = new AtomicLong(0L);
		this.clearCount = new AtomicLong(0L);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public long getCacheHitCount() {
		return this.hitCount.get();
	}

	@Override
	public long getCacheMissCount() {
		return this.missCount.get();
	}

	@Override
	public long getEvictionCount() {
		return this.evictedCount.get();
	}

	@Override
	public long getRollbackCount() {
		return rollbackCount.get();
	}

	@Override
	public long getClearCount() {
		return clearCount.get();
	}

	public CacheStatisticsImpl duplicate() {
		CacheStatisticsImpl clone = new CacheStatisticsImpl();
		clone.hitCount.set(this.getCacheHitCount());
		clone.missCount.set(this.getCacheMissCount());
		clone.evictedCount.set(this.getEvictionCount());
		clone.rollbackCount.set(this.getRollbackCount());
		clone.clearCount.set(this.getClearCount());
		return clone;
	}

	// =====================================================================================================================
	// INTERNAL API
	// =====================================================================================================================

	public void registerHit() {
		this.hitCount.incrementAndGet();
	}

	public void registerMiss() {
		this.missCount.incrementAndGet();
	}

	public void registerEviction(){
		this.evictedCount.incrementAndGet();
	}

	public void registerEvictions(long amount){
		this.evictedCount.addAndGet(amount);
	}

	public void registerRollback(){
		this.rollbackCount.incrementAndGet();
	}

	public void registerClear(){
		this.clearCount.incrementAndGet();
	}

	public void reset() {
		this.hitCount.set(0);
		this.missCount.set(0);
	}

	// =====================================================================================================================
	// TO STRING
	// =====================================================================================================================

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("CacheStatistics[Hit: ");
		builder.append(this.getCacheHitCount());
		builder.append(", Miss: ");
		builder.append(this.getCacheMissCount());
		builder.append(", Hit Ratio: ");
		builder.append(this.getCacheHitRatio() * 100.0);
		builder.append("%]");
		return builder.toString();
	}

}
