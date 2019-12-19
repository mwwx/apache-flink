package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.JdbcValidator;
import org.apache.flink.table.descriptors.LookupValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_DRIVER;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_INCREASE_COLUMN;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_OFFSET;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_QUOTING;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_FETCH_SIZE;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_COLUMN;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_LOWER_BOUND;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_NUM;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_PARTITION_UPPER_BOUND;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_READ_QUERY;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_TYPE_VALUE_JDBC;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_URL;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_USERNAME;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.ORIGIN_SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link JdbcTableSource}.
 */
public class JdbcTableSourceFactory implements StreamTableSourceFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_JDBC); // jdbc
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// common options
		properties.add(CONNECTOR_DRIVER);
		properties.add(CONNECTOR_URL);
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_USERNAME);
		properties.add(CONNECTOR_PASSWORD);
		properties.add(CONNECTOR_QUOTING);

		// scan options
		properties.add(CONNECTOR_READ_QUERY);
		properties.add(CONNECTOR_READ_PARTITION_COLUMN);
		properties.add(CONNECTOR_READ_PARTITION_NUM);
		properties.add(CONNECTOR_READ_PARTITION_LOWER_BOUND);
		properties.add(CONNECTOR_READ_PARTITION_UPPER_BOUND);
		properties.add(CONNECTOR_READ_FETCH_SIZE);
		properties.add(CONNECTOR_INCREASE_COLUMN);
		properties.add(CONNECTOR_OFFSET);

		// lookup options
		properties.addAll(LookupValidator.getProperties());

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		// origin
		properties.add(ORIGIN_SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(ORIGIN_SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(ORIGIN_SCHEMA + ".#." + SCHEMA_NAME);

		// time attributes
		properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

		// format wildcard
		properties.add(FormatDescriptorValidator.FORMAT + ".*");

		return properties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema schema = TableSchemaUtils.getPhysicalSchema(
			descriptorProperties.getTableSchema(SCHEMA));

		String proctimeAttr = SchemaValidator.deriveProctimeAttribute(descriptorProperties).orElse(null);
		List<RowtimeAttributeDescriptor> rowtimeAttr = SchemaValidator.deriveRowtimeAttributes(descriptorProperties);
		// origin schema
		TableSchema originSchema = descriptorProperties.getOptionalTableSchema(ORIGIN_SCHEMA).orElse(schema);

		Map<String, String> fieldMapping = SchemaValidator.deriveFieldMapping(
			descriptorProperties, Optional.of(originSchema.toRowType()));

		return JdbcTableSource.builder()
			.setOptions(JdbcUtils.getJdbcOptions(descriptorProperties))
			.setReadOptions(getJdbcReadOptions(descriptorProperties))
			.setLookupOptions(LookupValidator.getLookupOptions(descriptorProperties))
			.setSchema(schema)
			.setOriginSchema(originSchema)
			.setRowtimeAttributeDescriptors(rowtimeAttr)
			.setProctimeAttribute(proctimeAttr)
			.setFieldMapping(fieldMapping)
			.build();
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new SchemaValidator(true, false, false)
			.validate(descriptorProperties);
		new JdbcValidator().validateSource(descriptorProperties);

		return descriptorProperties;
	}

	private JdbcReadOptions getJdbcReadOptions(DescriptorProperties descriptorProperties) {
		final Optional<String> query = descriptorProperties.getOptionalString(CONNECTOR_READ_QUERY);
		final Optional<String> partitionColumnName =
			descriptorProperties.getOptionalString(CONNECTOR_READ_PARTITION_COLUMN);
		final Optional<Long> partitionLower = descriptorProperties.getOptionalLong(CONNECTOR_READ_PARTITION_LOWER_BOUND);
		final Optional<Long> partitionUpper = descriptorProperties.getOptionalLong(CONNECTOR_READ_PARTITION_UPPER_BOUND);
		final Optional<Integer> numPartitions = descriptorProperties.getOptionalInt(CONNECTOR_READ_PARTITION_NUM);

		final JdbcReadOptions.Builder builder = JdbcReadOptions.builder();
		if (query.isPresent()) {
			builder.setQuery(query.get());
		}
		if (partitionColumnName.isPresent()) {
			builder.setPartitionColumnName(partitionColumnName.get());
			builder.setPartitionLowerBound(partitionLower.get());
			builder.setPartitionUpperBound(partitionUpper.get());
			builder.setNumPartitions(numPartitions.get());
		}
		descriptorProperties.getOptionalInt(CONNECTOR_READ_FETCH_SIZE).ifPresent(builder::setFetchSize);

		return builder.build();
	}
}
