package org.apache.flink.table.descriptors;

import org.apache.flink.table.sources.TableDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * LookupableTableDescriptor.
 */
public class LookupableTableDescriptor extends ConnectorDescriptor {

	/**
	 * 表的类型,是流表还是维表.
	 */
	protected String dataType = TableDataType.STREAM.name().toUpperCase();

	/**
	 * 缓存策略，现在有all,lru,none.
	 */
	protected String cacheStrategy;

	/**
	 * 缓存类型，现在有memory和file默认memory.
	 */
	protected String cacheType;

	/**
	 * 缓存大小.
	 */
	protected String cacheSize;

	/**
	 * 缓存过期时间.
	 */
	protected String cacheTTLMs;

	/**
	 * 最大重试次数.
	 */
	protected String maxRetryTimes;

	// async config

	/**
	 * 是否开启异步请求数据,默认不开启.
	 */
	protected String asyncEnabled;

	/**
	 * 是否按顺序等待顺序,默认按顺序等.
	 */
	protected String orderedWait;

	/**
	 * 等待超时时间.
	 */
	protected String waitTimeout;

	/**
	 * 等待队列的大小.
	 */
	protected String waitQueueCapacity;

	public LookupableTableDescriptor(ConnectorDescriptor connectorDescriptor) {
		super(connectorDescriptor.getType(), connectorDescriptor.getVersion(), connectorDescriptor.isFormatNeeded());
	}

	public LookupableTableDescriptor setDataType(String dataType) {
		this.dataType = dataType;
		return this;
	}

	public LookupableTableDescriptor setCacheStrategy(String cacheStrategy) {
		this.cacheStrategy = cacheStrategy;
		return this;
	}

	public LookupableTableDescriptor setCacheType(String cacheType) {
		this.cacheType = cacheType;
		return this;
	}

	public LookupableTableDescriptor setCacheSize(String cacheSize) {
		this.cacheSize = cacheSize;
		return this;
	}

	public LookupableTableDescriptor setCacheTTLMs(String cacheTTLMs) {
		this.cacheTTLMs = cacheTTLMs;
		return this;
	}

	public LookupableTableDescriptor setMaxRetryTimes(String maxRetryTimes) {
		this.maxRetryTimes = maxRetryTimes;
		return this;
	}

	public LookupableTableDescriptor setAsyncEnabled(String asyncEnabled) {
		this.asyncEnabled = asyncEnabled;
		return this;
	}

	public LookupableTableDescriptor setOrderedWait(String orderedWait) {
		this.orderedWait = orderedWait;
		return this;
	}

	public LookupableTableDescriptor setWaitTimeout(String waitTimeout) {
		this.waitTimeout = waitTimeout;
		return this;
	}

	public LookupableTableDescriptor setWaitQueueCapacity(String waitQueueCapacity) {
		this.waitQueueCapacity = waitQueueCapacity;
		return this;
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		Map<String, String> properties = new HashMap<>();

		if (cacheStrategy != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_STRATEGY, cacheStrategy);
		}

		if (cacheType != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_TYPE, cacheType);
		}

		if (cacheSize != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS, cacheSize);
		}

		if (cacheTTLMs != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_CACHE_TTL, cacheTTLMs);
		}

		if (maxRetryTimes != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_MAX_RETRIES, maxRetryTimes);
		}

		if (asyncEnabled != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_ASYNC_ENABLED, asyncEnabled);
		}

		if (orderedWait != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_ASYNC_ORDERED_WAIT, orderedWait);
		}

		if (waitTimeout != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_ASYNC_WAIT_TIMEOUT, waitTimeout);
		}

		if (waitQueueCapacity != null) {
			properties.put(LookupValidator.CONNECTOR_LOOKUP_ASYNC_WAIT_QUEUE_CAPACITY, waitQueueCapacity);
		}

		return properties;
	}
}
