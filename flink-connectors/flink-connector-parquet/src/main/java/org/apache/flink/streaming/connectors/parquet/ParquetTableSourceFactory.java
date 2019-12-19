package org.apache.flink.streaming.connectors.parquet;

import org.apache.flink.streaming.connectors.parquet.ParquetTableSource.ParquetTableSourceBuilder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.descriptors.LookupOptionsValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.parquet.ParquetSourceValidator.FORMAT_TYPE_VALUE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.RowtimeValidator.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.SchemaValidator.SCHEMA_TYPE;

/**
 * ParquetTableSource Factory.
 */
public class ParquetTableSourceFactory implements StreamTableSourceFactory<Row> {

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA());

		long fieldCount = properties.keySet().stream().filter(k -> k.startsWith(SCHEMA()) && k.endsWith(".name")).count();

		String proctimeAttr = SchemaValidator.deriveProctimeAttribute(descriptorProperties).orElse(null);
		if (!StringUtils.isNullOrWhitespaceOnly(proctimeAttr)) {
			fieldCount -= 1;
		}

		TableSchema.Builder schemaBuilder = TableSchema.builder();
		for (int i = 0; i < fieldCount; i++) {
			String name = String.format("origin.%d", i);
			String type = String.format("%s.%d.type", SCHEMA(), i);

			String pName = properties.get(name);
			if (pName == null) {
				throw new ValidationException(String.format("Invalid table schema. Could not find name for field '%s.%d'.", SCHEMA(), i));
			}

			String pType = properties.get(type);
			if (pType == null) {
				throw new ValidationException(String.format("Invalid table schema. Could not find type for field '%s.%d'.", SCHEMA(), i));
			}

			schemaBuilder.field(pName, TypeStringUtils.readTypeInfo(pType));
		}

		TableSchema originTableSchema = schemaBuilder.build();

		ParquetTableSourceBuilder builder = new ParquetTableSourceBuilder();
		builder.path(descriptorProperties.getString(FileSystemValidator.CONNECTOR_PATH()))
			.fields(tableSchema.getFieldNames(), tableSchema.getFieldTypes())
			.proctimeAttribute(SchemaValidator.deriveProctimeAttribute(descriptorProperties)
				.orElse(null))
			.rowtimeAttributeDescriptors(SchemaValidator.deriveRowtimeAttributes(descriptorProperties))
			.originTableSchema(originTableSchema);

		return builder.build();
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new ParquetSourceValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	@Override
	public Map<String, String> requiredContext() {
		HashMap<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, FileSystemValidator.CONNECTOR_TYPE_VALUE());
		context.put(FORMAT_TYPE, FORMAT_TYPE_VALUE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		context.put(FORMAT_PROPERTY_VERSION, "1");
		context.put(StreamTableDescriptorValidator.UPDATE_MODE(), StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND());
		context.put(CONNECTOR_VERSION, "2");
		return context;
	}

	@Override
	public List<String> supportedProperties() {

		List<String> properties = new ArrayList<>();

		// connector
		properties.add(FileSystemValidator.CONNECTOR_PATH());

		properties.add(ParquetSourceValidator.CONNECTOR_DATA_TYPE);
		properties.add(ParquetSourceValidator.CONNECTOR_CACHE_TYPE);

		//cache support
		properties.add(LookupOptionsValidator.LOOKUP_CONFIG_CACHE_STRATEGY);

		// schema
		properties.add(SCHEMA() + ".#." + SCHEMA_TYPE());
		properties.add(SCHEMA() + ".#." + SCHEMA_NAME());
		properties.add(SCHEMA() + ".#." + SCHEMA_FROM());

		properties.add("origin.#");

		// time attributes
		properties.add(SCHEMA() + ".#." + SCHEMA_PROCTIME());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_TYPE());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_FROM());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_CLASS());
		properties.add(SCHEMA() + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_TYPE());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_CLASS());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_SERIALIZED());
		properties.add(SCHEMA() + ".#." + ROWTIME_WATERMARKS_DELAY());

		return properties;
	}
}
