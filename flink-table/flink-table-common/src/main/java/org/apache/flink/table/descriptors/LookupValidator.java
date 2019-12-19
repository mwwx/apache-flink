package org.apache.flink.table.descriptors;

import org.apache.flink.table.sources.TableDataType;
import org.apache.flink.table.sources.lookup.LookupOptions;
import org.apache.flink.table.sources.lookup.cache.CacheStrategy;
import org.apache.flink.table.sources.lookup.cache.CacheType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * LookupValidator.
 */
public class LookupValidator implements DescriptorValidator {

	public static final String CONNECTOR_DATA_TYPE = "connector.data.type";
	public static final String CONNECTOR_LOOKUP_CACHE_TYPE = "lookup.cache.type";
	public static final String CONNECTOR_LOOKUP_CACHE_STRATEGY = "lookup.cache.strategy";
	public static final String CONNECTOR_LOOKUP_CACHE_MAX_ROWS = "lookup.cache.max-rows";
	public static final String CONNECTOR_LOOKUP_CACHE_TTL = "lookup.cache.ttl";
	public static final String CONNECTOR_LOOKUP_MAX_RETRIES = "lookup.cache.max-retries";
	public static final String CONNECTOR_LOOKUP_ASYNC_ENABLED = "lookup.cache.async.enabled";
	public static final String CONNECTOR_LOOKUP_ASYNC_ORDERED_WAIT = "lookup.cache.async.ordered-wait";
	public static final String CONNECTOR_LOOKUP_ASYNC_WAIT_TIMEOUT = "lookup.cache.async.wait.timeout";
	public static final String CONNECTOR_LOOKUP_ASYNC_WAIT_QUEUE_CAPACITY = "lookup.cache.async.wait.queue-capacity";
	public static final String CONNECTOR_LOOKUP_CACHE_SEGMENT_SIZE = "lookup.cache.segment-size";

	public static List<String> getProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_DATA_TYPE);
		properties.add(CONNECTOR_LOOKUP_CACHE_TYPE);
		properties.add(CONNECTOR_LOOKUP_CACHE_STRATEGY);
		properties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
		properties.add(CONNECTOR_LOOKUP_CACHE_TTL);
		properties.add(CONNECTOR_LOOKUP_MAX_RETRIES);
		properties.add(CONNECTOR_LOOKUP_ASYNC_ENABLED);
		properties.add(CONNECTOR_LOOKUP_ASYNC_ORDERED_WAIT);
		properties.add(CONNECTOR_LOOKUP_ASYNC_WAIT_TIMEOUT);
		properties.add(CONNECTOR_LOOKUP_ASYNC_WAIT_QUEUE_CAPACITY);
		properties.add(CONNECTOR_LOOKUP_CACHE_SEGMENT_SIZE);
		return properties;
	}

	public static LookupOptions getLookupOptions(DescriptorProperties descriptorProperties) {
		LookupOptions.Builder builder = LookupOptions.builder();
		String dataTypeString = descriptorProperties.getOptionalString(CONNECTOR_DATA_TYPE).orElse(TableDataType.STREAM.name()).toUpperCase();
		TableDataType dataType = TableDataType.valueOf(dataTypeString);
		if (TableDataType.STATIC == dataType) {
			CacheStrategy cacheStrategy = CacheStrategy.valueOf(descriptorProperties.getString(CONNECTOR_LOOKUP_CACHE_STRATEGY).toUpperCase());
			builder.setCacheStrategy(cacheStrategy);
			switch (cacheStrategy) {
				case ALL: {
					builder.setCacheType(CacheType.valueOf(descriptorProperties
						.getString(CONNECTOR_LOOKUP_CACHE_TYPE)
						.toUpperCase()));
					builder.setCacheIncrement(descriptorProperties
						.getOptionalLong(CONNECTOR_LOOKUP_CACHE_SEGMENT_SIZE)
						.orElse(LookupOptions.Builder.DEFAULT_CACHE_INCREMENT));
					break;
				}
				case LRU: {
					builder.setCacheType(CacheType.valueOf(descriptorProperties.getString(CONNECTOR_LOOKUP_CACHE_TYPE).toUpperCase()));
					builder.setCacheSize(descriptorProperties.getLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS));
					builder.setCacheTTLMs(descriptorProperties.getDuration(CONNECTOR_LOOKUP_CACHE_TTL).toMillis());
					descriptorProperties.getOptionalBoolean(CONNECTOR_LOOKUP_ASYNC_ENABLED).ifPresent(builder::setAsyncEnabled);
					descriptorProperties.getOptionalBoolean(CONNECTOR_LOOKUP_ASYNC_ORDERED_WAIT).ifPresent(builder::setOrderedWait);
					descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_ASYNC_WAIT_TIMEOUT).ifPresent(builder::setWaitTimeout);
					descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_ASYNC_WAIT_QUEUE_CAPACITY).ifPresent(builder::setWaitQueueCapacity);
					break;
				}
				case NONE: {
					break;
				}
				default:
					throw new UnsupportedOperationException("unsupported cacheStrategy: " + cacheStrategy);
			}
			descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);
		}
		return builder.build();
	}

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_DATA_TYPE, true, 1, 20);
		String dataTypeString = properties.getOptionalString(CONNECTOR_DATA_TYPE).orElse(TableDataType.STREAM.name()).toUpperCase();
		TableDataType dataType = TableDataType.valueOf(dataTypeString);
		if (TableDataType.STATIC == dataType) {
			properties.validateString(CONNECTOR_LOOKUP_CACHE_STRATEGY, false, 1, 20);
			Optional<String> optionalCacheStrategy = properties.getOptionalString(CONNECTOR_LOOKUP_CACHE_STRATEGY);
			if (optionalCacheStrategy.isPresent()) {
				CacheStrategy cacheStrategy = CacheStrategy.valueOf(properties.getString(CONNECTOR_LOOKUP_CACHE_STRATEGY).toUpperCase());
				switch (cacheStrategy) {
					case ALL: {
						properties.validateString(CONNECTOR_LOOKUP_CACHE_TYPE, false, 1, 20);
						properties.validateLong(CONNECTOR_LOOKUP_CACHE_SEGMENT_SIZE, true);
						break;
					}
					case LRU: {
						properties.validateString(CONNECTOR_LOOKUP_CACHE_TYPE, false, 1, 20);
						properties.validateInt(CONNECTOR_LOOKUP_MAX_RETRIES, true);
						properties.validateInt(CONNECTOR_LOOKUP_CACHE_MAX_ROWS, false);
						properties.validateDuration(CONNECTOR_LOOKUP_CACHE_TTL, false, 1);
						properties.validateBoolean(CONNECTOR_LOOKUP_ASYNC_ENABLED, true);
						properties.validateBoolean(CONNECTOR_LOOKUP_ASYNC_ORDERED_WAIT, true);
						properties.validateInt(CONNECTOR_LOOKUP_ASYNC_WAIT_TIMEOUT, true);
						properties.validateInt(CONNECTOR_LOOKUP_ASYNC_WAIT_QUEUE_CAPACITY, true);
						break;
					}
					case NONE: {
						properties.validateInt(CONNECTOR_LOOKUP_MAX_RETRIES, true);
						break;
					}
					default:
						throw new UnsupportedOperationException("unsupported cacheStrategy: " + cacheStrategy);
				}
			}
		}
	}
}
