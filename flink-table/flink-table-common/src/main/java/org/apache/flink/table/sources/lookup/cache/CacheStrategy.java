package org.apache.flink.table.sources.lookup.cache;

/**
 * cache strategy for lookup.
 */
public enum CacheStrategy {

	ALL(new AllCache()), LRU(new LRUCache()), NONE(new NoneCache());

	private Cache cache;

	CacheStrategy(Cache cache) {
		this.cache = cache;
	}

	public Cache getCache() {
		return cache.newInstance();
	}

	public CacheStrategy setCache(Cache cache) {
		this.cache = cache;
		return this;
	}

}
