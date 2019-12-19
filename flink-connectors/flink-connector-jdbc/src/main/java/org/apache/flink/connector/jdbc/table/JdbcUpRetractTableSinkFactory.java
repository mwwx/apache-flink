/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JdbcValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
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
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_RETRACT;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_UPSERT;

/**
 * Factory for creating configured instances of {@link JdbcRetractTableSink} and {@link JdbcUpsertTableSink}.
 */
public abstract class JdbcUpRetractTableSinkFactory implements
	StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	public abstract String getUpdateMode();

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_JDBC); // jdbc
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		context.put(UPDATE_MODE, getUpdateMode());
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

		// sink options
		properties.add(CONNECTOR_WRITE_FLUSH_MAX_ROWS);
		properties.add(CONNECTOR_WRITE_FLUSH_INTERVAL);
		properties.add(CONNECTOR_WRITE_MAX_RETRIES);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		// table constraint
		properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_NAME);
		properties.add(SCHEMA + "." + DescriptorProperties.PRIMARY_KEY_COLUMNS);

		return properties;
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema schema = TableSchemaUtils.getPhysicalSchema(
			descriptorProperties.getTableSchema(SCHEMA));

		String updateMode = properties.getOrDefault(UPDATE_MODE, UPDATE_MODE_VALUE_UPSERT);

		switch (updateMode) {
			case UPDATE_MODE_VALUE_UPSERT: {
				final JdbcUpsertTableSink.Builder builder = JdbcUpsertTableSink
					.builder()
					.setOptions(JdbcUtils.getJdbcOptions(descriptorProperties))
					.setTableSchema(schema);

				descriptorProperties.getOptionalInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS).ifPresent(builder::setFlushMaxSize);
				descriptorProperties.getOptionalDuration(CONNECTOR_WRITE_FLUSH_INTERVAL).ifPresent(
					s -> builder.setFlushIntervalMills(s.toMillis()));
				descriptorProperties.getOptionalInt(CONNECTOR_WRITE_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);

				return builder.build();
			}

			case UPDATE_MODE_VALUE_RETRACT: {
				final JdbcRetractTableSink.Builder builder = JdbcRetractTableSink
					.builder()
					.setOptions(JdbcUtils.getJdbcOptions(descriptorProperties))
					.setTableSchema(schema);

				descriptorProperties.getOptionalInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS).ifPresent(builder::setFlushMaxSize);
				descriptorProperties.getOptionalDuration(CONNECTOR_WRITE_FLUSH_INTERVAL).ifPresent(
					s -> builder.setFlushIntervalMills(s.toMillis()));
				descriptorProperties.getOptionalInt(CONNECTOR_WRITE_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);

				return builder.build();
			}

			default: {
				throw new RuntimeException("unsupported update-mode: " + updateMode);
			}
		}
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new SchemaValidator(true, false, false)
			.validate(descriptorProperties);
		new JdbcValidator().validateSink(descriptorProperties);

		return descriptorProperties;
	}
}
