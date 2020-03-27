package org.apache.flink.table.sources.lookup;

import org.apache.flink.table.descriptors.LookupValidator;
import org.apache.flink.table.sources.TableDataType;
import org.apache.flink.table.sources.lookup.cache.CacheStrategy;
import org.apache.flink.table.sources.lookup.cache.CacheType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Options for lookup.
 */
public class LookupOptions implements Serializable {

	private LookupOptions() {

	}

	/**
	 * 表的类型,是流表还是维表.
	 */
	protected TableDataType dataType;

	/**
	 * 缓存类型，现在有memory和file默认memory.
	 */
	protected CacheType cacheType;

	/**
	 * 缓存策略，现在有all,lru,none.
	 */
	protected CacheStrategy cacheStrategy;

	/**
	 * 缓存大小.
	 */
	protected Long cacheSize;

	/**
	 * 缓存过期时间.
	 */
	protected Long cacheTTLMs;

	/**
	 * 最大重试次数.
	 */
	protected Integer maxRetryTimes;

	// async config

	/**
	 * 是否开启异步请求数据,默认不开启.
	 */
	protected Boolean asyncEnabled;

	/**
	 * 是否按顺序等待顺序,默认按顺序等.
	 */
	protected Boolean orderedWait;

	/**
	 * 等待超时时间.
	 */
	protected Long waitTimeout;

	/**
	 * 等待队列的大小.
	 */
	protected Integer waitQueueCapacity;

	/**
	 * 缓存扩容时,每次增量大小,默认128M.
	 */
	protected Long cacheIncrement = 128 * 1024 * 1024L;

	public TableDataType getDataType() {
		return dataType;
	}

	private LookupOptions setDataType(TableDataType dataType) {
		this.dataType = dataType;
		return this;
	}

	public CacheType getCacheType() {
		return cacheType;
	}

	private LookupOptions setCacheType(CacheType cacheType) {
		this.cacheType = cacheType;
		return this;
	}

	public CacheStrategy getCacheStrategy() {
		return cacheStrategy;
	}

	private LookupOptions setCacheStrategy(CacheStrategy cacheStrategy) {
		this.cacheStrategy = cacheStrategy;
		return this;
	}

	public Long getCacheSize() {
		return cacheSize;
	}

	private LookupOptions setCacheSize(Long cacheSize) {
		this.cacheSize = cacheSize;
		return this;
	}

	public Long getCacheTTLMs() {
		return cacheTTLMs;
	}

	private LookupOptions setCacheTTLMs(Long cacheTTLMs) {
		this.cacheTTLMs = cacheTTLMs;
		return this;
	}

	public Integer getMaxRetryTimes() {
		return maxRetryTimes;
	}

	private LookupOptions setMaxRetryTimes(Integer maxRetryTimes) {
		this.maxRetryTimes = maxRetryTimes;
		return this;
	}

	public Boolean isAsyncEnabled() {
		return asyncEnabled;
	}

	private LookupOptions setAsyncEnabled(Boolean asyncEnabled) {
		this.asyncEnabled = asyncEnabled;
		return this;
	}

	public Boolean isOrderedWait() {
		return orderedWait;
	}

	private LookupOptions setOrderedWait(Boolean orderedWait) {
		this.orderedWait = orderedWait;
		return this;
	}

	public Long getWaitTimeout() {
		return waitTimeout;
	}

	private LookupOptions setWaitTimeout(Long waitTimeout) {
		this.waitTimeout = waitTimeout;
		return this;
	}

	public Integer getWaitQueueCapacity() {
		return waitQueueCapacity;
	}

	private LookupOptions setWaitQueueCapacity(Integer waitQueueCapacity) {
		this.waitQueueCapacity = waitQueueCapacity;
		return this;
	}

	public Long getCacheIncrement() {
		return cacheIncrement;
	}

	public void setCacheIncrement(Long cacheIncrement) {
		this.cacheIncrement = cacheIncrement;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LookupOptions that = (LookupOptions) o;
		return cacheType == that.cacheType &&
			cacheStrategy == that.cacheStrategy &&
			dataType == that.dataType &&
			Objects.equals(cacheSize, that.cacheSize) &&
			Objects.equals(cacheTTLMs, that.cacheTTLMs) &&
			Objects.equals(maxRetryTimes, that.maxRetryTimes) &&
			Objects.equals(asyncEnabled, that.asyncEnabled) &&
			Objects.equals(orderedWait, that.orderedWait) &&
			Objects.equals(waitTimeout, that.waitTimeout) &&
			Objects.equals(waitQueueCapacity, that.waitQueueCapacity) &&
			Objects.equals(cacheIncrement, that.cacheIncrement);
	}

	@Override
	public int hashCode() {
		return Objects.hash(cacheType, cacheStrategy, dataType, cacheSize, cacheTTLMs, maxRetryTimes,
			asyncEnabled, orderedWait, waitTimeout, waitQueueCapacity, cacheIncrement);
	}

	public Map<String, String> toProperties() {
		Map<String, String> properties = new HashMap<>();
		if (cacheType != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_TYPE, cacheType.name());
		}
		if (cacheStrategy != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_STRATEGY, cacheStrategy.name());
		}
		if (dataType != null) {
			properties.put(LookupValidator.CONNECTOR_DATA_TYPE, dataType.name());
		}
		if (cacheSize != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS, cacheSize.toString());
		}
		if (cacheTTLMs != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_TTL, cacheTTLMs.toString());
		}
		if (maxRetryTimes != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_MAX_RETRIES, maxRetryTimes.toString());
		}
		if (asyncEnabled != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_ASYNC_ENABLED, asyncEnabled.toString());
		}
		if (orderedWait != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_ASYNC_ORDERED_WAIT, orderedWait.toString());
		}
		if (waitTimeout != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_ASYNC_WAIT_TIMEOUT, waitTimeout.toString());
		}
		if (waitQueueCapacity != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_ASYNC_WAIT_QUEUE_CAPACITY, waitQueueCapacity.toString());
		}
		if (cacheIncrement != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_SEGMENT_SIZE, cacheIncrement.toString());
		}
		return properties;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * builder for LookupOptions.
	 */
	public static final class Builder {
		public static final Long DEFAULT_CACHE_INCREMENT = 128 * 1024 * 1024L;

		protected TableDataType dataType;
		protected CacheType cacheType = CacheType.FILE;
		protected CacheStrategy cacheStrategy = CacheStrategy.ALL;
		protected Long cacheSize = -1L;
		protected Long cacheTTLMs = -1L;
		protected Integer maxRetryTimes = 3;
		protected Boolean asyncEnabled = false;
		protected Boolean orderedWait = false;
		protected Long waitTimeout = 3000000L;
		protected Integer waitQueueCapacity = 50;
		private Long cacheIncrement = DEFAULT_CACHE_INCREMENT;

		private Builder() {
		}

		public Builder setDataType(TableDataType dataType) {
			this.dataType = dataType;
			return this;
		}

		public Builder setCacheType(CacheType cacheType) {
			this.cacheType = cacheType;
			return this;
		}

		public Builder setCacheStrategy(CacheStrategy cacheStrategy) {
			this.cacheStrategy = cacheStrategy;
			return this;
		}

		public Builder setCacheSize(long cacheSize) {
			this.cacheSize = cacheSize;
			return this;
		}

		public Builder setCacheTTLMs(long cacheTTLMs) {
			this.cacheTTLMs = cacheTTLMs;
			return this;
		}

		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public Builder setAsyncEnabled(boolean asyncEnabled) {
			this.asyncEnabled = asyncEnabled;
			return this;
		}

		public Builder setOrderedWait(boolean orderedWait) {
			this.orderedWait = orderedWait;
			return this;
		}

		public Builder setWaitTimeout(long waitTimeout) {
			this.waitTimeout = waitTimeout;
			return this;
		}

		public Builder setWaitQueueCapacity(int waitQueueCapacity) {
			this.waitQueueCapacity = waitQueueCapacity;
			return this;
		}

		public Builder setCacheIncrement(long cacheIncrement) {
			this.cacheIncrement = cacheIncrement;
			return this;
		}

		public LookupOptions build() {
			LookupOptions lookupOptions = new LookupOptions();
			lookupOptions.setDataType(dataType);
			lookupOptions.setCacheType(cacheType);
			lookupOptions.setCacheStrategy(cacheStrategy);
			lookupOptions.setCacheSize(cacheSize);
			lookupOptions.setCacheTTLMs(cacheTTLMs);
			lookupOptions.setMaxRetryTimes(maxRetryTimes);
			lookupOptions.setAsyncEnabled(asyncEnabled);
			lookupOptions.setOrderedWait(orderedWait);
			lookupOptions.setWaitTimeout(waitTimeout);
			lookupOptions.setWaitQueueCapacity(waitQueueCapacity);
			lookupOptions.setCacheIncrement(cacheIncrement);
			return lookupOptions;
		}
	}
}
