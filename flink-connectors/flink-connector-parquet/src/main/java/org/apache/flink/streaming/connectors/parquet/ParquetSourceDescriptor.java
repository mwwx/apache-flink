package org.apache.flink.streaming.connectors.parquet;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.Map;

/**
 * ParquetSourceDescriptor.
 */
public class ParquetSourceDescriptor extends FormatDescriptor {


	private String[] originColumnNames;
	private String dataType;
	private String cacheType;

	/**
	 * Constructs a {@link FormatDescriptor}.
	 *
	 */
	public ParquetSourceDescriptor() {
		super(ParquetSourceValidator.FORMAT_TYPE_VALUE, 1);
	}

	public ParquetSourceDescriptor originColumnNames(String[] originColumnNames) {
		this.originColumnNames = originColumnNames;
		return this;
	}

	public ParquetSourceDescriptor dataType(String dataType) {
		this.dataType = dataType;
		return this;
	}

	public ParquetSourceDescriptor cacheType(String cacheType) {
		this.cacheType = cacheType;
		return this;
	}

	@Override
	protected Map<String, String> toFormatProperties() {

		DescriptorProperties properties = new DescriptorProperties();
		if (originColumnNames != null && originColumnNames.length > 0) {
			for (int i = 0; i < originColumnNames.length; i++) {
				properties.putString("origin." + i, originColumnNames[i]);
			}
		}

		if (dataType == null) {
			properties.putString(
				ParquetSourceValidator.CONNECTOR_DATA_TYPE, ParquetSourceValidator.CONNECTOR_DATA_TYPE_VALUE);
		} else {
			properties.putString(ParquetSourceValidator.CONNECTOR_DATA_TYPE, dataType);
		}

		if (cacheType != null) {
			properties.putString(ParquetSourceValidator.CONNECTOR_CACHE_TYPE, cacheType);
		}

		return properties.asMap();
	}
}
