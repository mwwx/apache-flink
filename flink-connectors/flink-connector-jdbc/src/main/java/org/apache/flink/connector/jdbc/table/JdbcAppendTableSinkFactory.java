package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.dialect.CommonDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JdbcValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.jdbc.utils.JdbcTypeUtil.normalizeTableSchema;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_DRIVER;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_QUOTING;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_TYPE_VALUE_JDBC;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_URL;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_USERNAME;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_WRITE_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_WRITE_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_WRITE_MAX_RETRIES;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * JDBCAppendTableSinkFactory.
 */
public class JdbcAppendTableSinkFactory implements StreamTableSinkFactory<Row> {

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new JdbcValidator().validateSink(descriptorProperties);

		return descriptorProperties;
	}

	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		TableSchema schema = normalizeTableSchema(tableSchema);

		JdbcOptions options = getJdbcOptions(descriptorProperties);

		JdbcAppendTableSink.Builder builder = JdbcAppendTableSink.builder()
			.setTableSchema(schema)
			.setOptions(options);

		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS).ifPresent(builder::setFlushMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_WRITE_FLUSH_INTERVAL)
			.ifPresent(e -> builder.setFlushIntervalMills(e.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);
		return builder.build();
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND);
		context.put(JdbcValidator.CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_JDBC); // jdbc
		context.put(JdbcValidator.CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// jdbc
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_DRIVER);
		properties.add(CONNECTOR_URL);
		properties.add(CONNECTOR_USERNAME);
		properties.add(CONNECTOR_PASSWORD);
		properties.add(CONNECTOR_QUOTING);
		properties.add(CONNECTOR_WRITE_FLUSH_MAX_ROWS);
		properties.add(JdbcValidator.CONNECTOR_WRITE_FLUSH_INTERVAL);
		properties.add(JdbcValidator.CONNECTOR_WRITE_MAX_RETRIES);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	private JdbcOptions getJdbcOptions(DescriptorProperties descriptorProperties) {
		final String url = descriptorProperties.getString(CONNECTOR_URL);
		final String driver = descriptorProperties.getString(CONNECTOR_DRIVER);
		//处理大小写
		String tableName = descriptorProperties.getString(CONNECTOR_TABLE);
		final JdbcOptions.Builder builder = JdbcOptions.builder()
			.setDBUrl(url)
			.setTableName(tableName)
			.setDialect(JdbcDialects.get(url).orElse(CommonDialect.newInstance(driver)));

		builder.setDriverName(driver);
		descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
		descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);
		descriptorProperties.getOptionalBoolean(CONNECTOR_QUOTING).ifPresent(builder::setIsQuoting);

		return builder.build();
	}
}
