package org.apache.flink.table.sources.lookup.cache;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.sources.lookup.DataFetcher;
import org.apache.flink.table.sources.lookup.LookupOptions;
import org.apache.flink.types.Row;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * base class for cache strategy impl.
 */
public abstract class Cache implements Serializable {

	protected LookupOptions lookupOptions;

	protected transient DB db;
	protected transient HTreeMap<Row, List<Row>> cache;

	protected DataFetcher fetcher;

	private boolean newInstance = false;

	protected Cache() {}

	public final void open(LookupOptions lookupOptions, DataFetcher fetcher, FunctionContext context) throws Exception {
		if (!newInstance) {
			throw new RuntimeException("please call newInstance before.");
		}
		this.lookupOptions = lookupOptions;
		this.fetcher = fetcher;
		fetcher.open();

		CacheType cacheType = lookupOptions.getCacheType();
		if (cacheType != null) {
			switch (cacheType) {
				case MEMORY: {
					db = DBMaker.memoryDB()
						.allocateIncrement(lookupOptions.getCacheIncrement())
						.executorEnable()
						.make();
					db.commit();
					break;
				}

				case HEAP: {
					db = DBMaker.heapDB()
						.allocateIncrement(lookupOptions.getCacheIncrement())
						.executorEnable()
						.make();
					db.commit();
					break;
				}

				case FILE: {
					String tmpDir = context.getJobParameter(CoreOptions.TMP_DIRS.key(), CoreOptions.TMP_DIRS.defaultValue());
					File dbFile = new File(tmpDir, UUID.randomUUID() + ".db");
					String dbFileName = dbFile.getAbsolutePath();
					db = DBMaker.fileDB(dbFileName)
						.fileMmapEnableIfSupported()
						.fileMmapPreclearDisable()
						.allocateIncrement(lookupOptions.getCacheIncrement())
						.fileChannelEnable()
						.closeOnJvmShutdown()
						.fileDeleteAfterClose()
						.make();
					db.commit();
					break;
				}
				default:
					throw new RuntimeException("Unsupported cacheType: " + cacheType);
			}
		}
		open();
	}

	protected abstract void open() throws Exception;

	protected abstract List<Row> getIfPresent(Row key) throws Exception;

	public void put(Row key, List<Row> value) {
		cache.put(key, value);
	}

	public List<Row> getOrDefault(Row key, List<Row> defaultValue) throws Exception {
		List<Row> res = getIfPresent(key);
		if (res == null) {
			return defaultValue;
		}
		return res;
	}

	public final Cache newInstance() {
		Cache cache = innerNewInstance();
		cache.newInstance = true;
		return cache;
	}

	protected abstract Cache innerNewInstance();

	public abstract void close() throws Exception;
}
