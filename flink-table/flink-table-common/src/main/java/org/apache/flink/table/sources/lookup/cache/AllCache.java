package org.apache.flink.table.sources.lookup.cache;

import org.apache.flink.types.Row;

import org.mapdb.DB;
import org.mapdb.HTreeMap;

import java.util.List;

/**
 * pre load all cache data before job start.
 */
public class AllCache extends Cache {

	protected AllCache() {
		super();
	}

	@Override
	public void open() throws Exception {
		DB.HashMapMaker maker =  db.hashMap("allCache");
		cache = (HTreeMap<Row, List<Row>>) maker.createOrOpen();
		fetcher.loadAllData(cache);
	}

	@Override
	public List<Row> getIfPresent(Row key) {
		return cache.get(key);
	}

	@Override
	public List<Row> getOrDefault(Row key, List<Row> defaultValue) {
		return cache.getOrDefault(key, defaultValue);
	}

	@Override
	protected Cache innerNewInstance() {
		return new AllCache();
	}

	@Override
	public void close() throws Exception {
		cache.close();
		db.close();
		fetcher.close();
	}
}
