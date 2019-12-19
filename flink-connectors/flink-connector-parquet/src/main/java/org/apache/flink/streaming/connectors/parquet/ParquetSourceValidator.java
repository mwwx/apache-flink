package org.apache.flink.streaming.connectors.parquet;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;

/**
 * ParquetValidator.
 */
public class ParquetSourceValidator extends FormatDescriptorValidator {

//	public static final String CONNECTOR_PATH = "path";
	public static final String FORMAT_TYPE_VALUE = "parquet";
	public static final String CONNECTOR_CACHE_TYPE = "cachetype";
	public static final String CONNECTOR_DATA_TYPE = "datatype";
	public static final String CONNECTOR_DATA_TYPE_VALUE = "stream";

	public static final String DEFAULT_CONNECTOR_CACHE_TYPE = "memory";

	public ParquetSourceValidator() {
		super();
	}

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		validateParquet(properties);
	}

	private void validateParquet(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateString(FileSystemValidator.CONNECTOR_PATH(), false, 1, Integer.MAX_VALUE);
		properties.validateValue(FORMAT_TYPE, FORMAT_TYPE_VALUE, false);
		properties.validateString(CONNECTOR_DATA_TYPE, true, 0, Integer.MAX_VALUE);
		properties.validateString(CONNECTOR_CACHE_TYPE, true, 0, Integer.MAX_VALUE);
	}

}
