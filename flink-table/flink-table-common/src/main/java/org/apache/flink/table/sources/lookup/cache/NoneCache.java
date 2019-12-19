package org.apache.flink.table.sources.lookup.cache;

import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * none cache,request data pre key.
 */
public class NoneCache extends Cache {

	protected NoneCache() {
		super();
	}

	@Override
	protected void open() throws Exception {
		//do nothing
	}

	@Override
	public List<Row> getIfPresent(Row key) throws Exception {
		List<Row> value = fetcher.syncRequestByKey(key);
		if (value == null) {
			value = new ArrayList<>();
		}
		return value;
	}

	@Override
	public void put(Row key, List<Row> value) {
		throw new UnsupportedOperationException("none cache doesn't support put data.");
	}

	@Override
	protected Cache innerNewInstance() {
		return new NoneCache();
	}

	@Override
	public void close() throws Exception {
		fetcher.close();
	}
}
