package org.apache.flink.table.sources.lookup;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.sources.lookup.cache.Cache;
import org.apache.flink.table.sources.lookup.cache.CacheStrategy;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/**
 * AsyncLookupTableFunction.
 */
public class AsyncLookupTableFunction extends AsyncTableFunction<Row> {

	private static final Logger logger = LoggerFactory.getLogger(AsyncLookupTableFunction.class);

	private LookupOptions lookupOptions;
	private DataFetcher fetcher;

	private int maxRetryTimes;

	private Cache cache;

	public AsyncLookupTableFunction(LookupOptions lookupOptions, DataFetcher fetcher) {
		this.lookupOptions = lookupOptions;
		this.fetcher = fetcher;
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
		if (!fetcher.supportAsync()) {
			throw new UnsupportedOperationException("fetcher doesn't support async request.");
		}
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		CacheStrategy cacheStrategy = lookupOptions.getCacheStrategy();
		this.cache = cacheStrategy.getCache();
		this.cache.open(lookupOptions, fetcher, context);
	}

	public void eval(CompletableFuture<Row> result, Object... keys) {
		CompletableFuture.supplyAsync(() -> {
			for (int retry = 1; retry <= maxRetryTimes; retry++) {
				try {
					return cache.getOrDefault(Row.of(keys), new ArrayList<>());
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
			throw new RuntimeException("fetch data error with run out of retries.");
		}).whenComplete((value, throwable) -> {
			if (throwable != null) {
				result.completeExceptionally(throwable);
			}
			for (Row row : value) {
				result.complete(row);
			}
		});
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return fetcher.getResultType();
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
