package org.apache.flink.table.sources.lookup.cache;

import org.apache.flink.types.Row;

import org.mapdb.DB;
import org.mapdb.HTreeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * lru cache for lookup.
 */
public class LRUCache extends Cache {

	protected LRUCache() {
		super();
	}

	@Override
	protected void open() throws Exception {
		DB.HashMapMaker maker =  db.hashMap("lruCache");
		long cacheSize = lookupOptions.getCacheSize();
		long cacheTTLMs = lookupOptions.getCacheTTLMs();
		maker = maker.expireMaxSize(cacheSize)
			.expireAfterCreate(cacheTTLMs, TimeUnit.MILLISECONDS)
			.expireAfterUpdate(cacheTTLMs, TimeUnit.MILLISECONDS)
			.expireAfterGet(cacheTTLMs, TimeUnit.MILLISECONDS);
		cache = (HTreeMap<Row, List<Row>>) maker.createOrOpen();
	}

	@Override
	public List<Row> getIfPresent(Row key) throws Exception {
		List<Row> value = cache.get(key);
		if (value == null) {
			value = fetcher.syncRequestByKey(key);
			if (value != null) {
				cache.put(key, value);
			} else {
				cache.put(key, new ArrayList<>());
			}
		}
		return value;
	}

	@Override
	protected Cache innerNewInstance() {
		return new LRUCache();
	}

	@Override
	public void close() throws Exception {
		cache.close();
		db.close();
		fetcher.close();
	}
}
