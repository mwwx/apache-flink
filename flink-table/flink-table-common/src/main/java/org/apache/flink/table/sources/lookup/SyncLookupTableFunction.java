package org.apache.flink.table.sources.lookup;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.lookup.cache.Cache;
import org.apache.flink.table.sources.lookup.cache.CacheStrategy;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * SyncLookupFunction.
 */
public class SyncLookupTableFunction extends TableFunction<Row> {

	private static final Logger logger = LoggerFactory.getLogger(SyncLookupTableFunction.class);

	private LookupOptions lookupOptions;
	private DataFetcher fetcher;
	private int maxRetryTimes;

	private Cache cache;

	public SyncLookupTableFunction(LookupOptions lookupOptions, DataFetcher fetcher) {
		this.lookupOptions = lookupOptions;
		this.fetcher = fetcher;
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
		Preconditions.checkArgument(this.maxRetryTimes > 0);
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		CacheStrategy cacheStrategy = lookupOptions.getCacheStrategy();
		logger.info("open {} with cacheStrategy {}", this.getClass().getName(), cacheStrategy);
		this.cache = cacheStrategy.getCache();
		this.cache.open(lookupOptions, fetcher, context);
	}

	public void eval(Object... keys) {
		for (int retry = 1; retry <= maxRetryTimes; retry++) {
			try {
				List<Row> values = cache.getOrDefault(Row.of(keys), new ArrayList<>());
				for (Row value : values) {
					collect(value);
				}
				break;
			} catch (Exception e) {
				logger.error(String.format("fetch data error, retry times = %d", retry), e);
				if (retry >= maxRetryTimes) {
					throw new RuntimeException("fetch data error with run out of retries.", e);
				}

				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return fetcher.getResultType();
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return fetcher.getParameterTypes(signature);
	}

	@Override
	public boolean isDeterministic() {
		return false;
	}

	@Override
	public void close() throws Exception {
		cache.close();
	}
}
