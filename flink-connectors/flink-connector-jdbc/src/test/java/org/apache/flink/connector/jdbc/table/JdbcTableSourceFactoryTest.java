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

import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.sources.lookup.LookupOptions;
import org.apache.flink.table.sources.lookup.cache.CacheStrategy;
import org.apache.flink.table.sources.lookup.cache.CacheType;
import org.apache.flink.table.types.DataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * JdbcTableSourceFactoryTest.
 */
public class JdbcTableSourceFactoryTest {

	private static final TableSchema schema = TableSchema.builder()
		.field("aaa", DataTypes.INT())
		.field("bbb", DataTypes.STRING())
		.field("ccc", DataTypes.DOUBLE())
		.field("ddd", DataTypes.DECIMAL(31, 18))
		.field("eee", DataTypes.TIMESTAMP(3))
		.build();

	@Test
	public void testJdbcTableSourceFactory() {
		Map<String, String> properties = getBasicProperties();
		properties.put("schema.#.rowtime.timestamps.type", "from-field");
		properties.put("schema.#.rowtime.timestamps.from", "eee");

		final StreamTableSource<?> actual = TableFactoryService.find(StreamTableSourceFactory.class, properties)
			.createStreamTableSource(properties);

		final JdbcOptions options = JdbcOptions.builder()
			.setDBUrl("jdbc:derby:memory:mydb")
			.setTableName("mytable")
			.setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
			.setUsername("user")
			.setPassword("pass")
			.build();
		Map<String, String> fieldMapping = new HashMap<>();
		fieldMapping.put("aaa", "aaa");
		fieldMapping.put("bbb", "bbb");
		fieldMapping.put("ccc", "ccc");
		fieldMapping.put("ddd", "ddd");
		fieldMapping.put("eee", "eee");
		final JdbcTableSource expected = JdbcTableSource.builder()
			.setOptions(options)
			.setSchema(schema)
			.setRowtimeAttributeDescriptors(new ArrayList<>())
			.setFieldMapping(fieldMapping)
			.build();

		TableSourceValidation.validateTableSource(expected, schema);
		TableSourceValidation.validateTableSource(actual, schema);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testJDBCReadProperties() {
		Map<String, String> properties = getBasicProperties();
		properties.put("connector.read.partition.column", "aaa");
		properties.put("connector.read.partition.lower-bound", "-10");
		properties.put("connector.read.partition.upper-bound", "100");
		properties.put("connector.read.partition.num", "10");
		properties.put("connector.read.fetch-size", "20");

		final StreamTableSource<?> actual = TableFactoryService.find(StreamTableSourceFactory.class, properties)
			.createStreamTableSource(properties);

		final JdbcOptions options = JdbcOptions.builder()
			.setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
			.setDBUrl("jdbc:derby:memory:mydb")
			.setUsername("user")
			.setPassword("pass")
			.setTableName("mytable")
			.build();
		final JdbcReadOptions readOptions = JdbcReadOptions.builder()
			.setPartitionColumnName("aaa")
			.setPartitionLowerBound(-10)
			.setPartitionUpperBound(100)
			.setNumPartitions(10)
			.setFetchSize(20)
			.build();

		Map<String, String> fieldMapping = new HashMap<>();
		fieldMapping.put("aaa", "aaa");
		fieldMapping.put("bbb", "bbb");
		fieldMapping.put("ccc", "ccc");
		fieldMapping.put("ddd", "ddd");
		fieldMapping.put("eee", "eee");

		final JdbcTableSource expected = JdbcTableSource.builder()
			.setOptions(options)
			.setReadOptions(readOptions)
			.setSchema(schema)
			.setRowtimeAttributeDescriptors(new ArrayList<>())
			.setFieldMapping(fieldMapping)
			.build();

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testJDBCLookupProperties() {
		Map<String, String> properties = getBasicProperties();
		properties.put("connector.data.type", "static");
		properties.put("lookup.cache.strategy", "all");
		properties.put("lookup.cache.type", "memory");
		properties.put("lookup.cache.max-retries", "10");

		final StreamTableSource<?> actual = TableFactoryService
			.find(StreamTableSourceFactory.class, properties)
			.createStreamTableSource(properties);

		final JdbcOptions options = JdbcOptions.builder()
			.setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
			.setDBUrl("jdbc:derby:memory:mydb")
			.setUsername("user")
			.setPassword("pass")
			.setTableName("mytable")
			.build();
		final LookupOptions lookupOptions = LookupOptions.builder()
			.setCacheStrategy(CacheStrategy.ALL)
			.setCacheType(CacheType.MEMORY)
			.setMaxRetryTimes(10)
			.build();
		Map<String, String> fieldMapping = new HashMap<>();
		fieldMapping.put("aaa", "aaa");
		fieldMapping.put("bbb", "bbb");
		fieldMapping.put("ccc", "ccc");
		fieldMapping.put("ddd", "ddd");
		fieldMapping.put("eee", "eee");
		final JdbcTableSource expected = JdbcTableSource.builder()
			.setOptions(options)
			.setLookupOptions(lookupOptions)
			.setSchema(schema)
			.setRowtimeAttributeDescriptors(new ArrayList<>())
			.setFieldMapping(fieldMapping)
			.build();

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testJDBCSinkProperties() {
		Map<String, String> properties = getBasicProperties();
		properties.put("update-mode", "upsert");
		properties.put("connector.write.flush.max-rows", "1000");
		properties.put("connector.write.flush.interval", "2min");
		properties.put("connector.write.max-retries", "5");

		final StreamTableSink<?> actual = TableFactoryService.find(StreamTableSinkFactory.class, properties)
			.createStreamTableSink(properties);

		final JdbcOptions options = JdbcOptions.builder()
			.setDBUrl("jdbc:derby:memory:mydb")
			.setTableName("mytable")
			.setUsername("user")
			.setPassword("pass")
			.build();
		final JdbcUpsertTableSink expected = JdbcUpsertTableSink.builder()
			.setOptions(options)
			.setTableSchema(schema)
			.setFlushMaxSize(1000)
			.setFlushIntervalMills(120_000)
			.setMaxRetryTimes(5)
			.build();

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testJDBCWithFilter() {
		Map<String, String> properties = getBasicProperties();
		properties.put("connector.driver", "org.apache.derby.jdbc.EmbeddedDriver");
		properties.put("connector.username", "user");
		properties.put("connector.password", "pass");

		final TableSource<?> actual = ((JdbcTableSource) TableFactoryService
			.find(StreamTableSourceFactory.class, properties)
			.createStreamTableSource(properties))
			.projectFields(new int[] {0, 2});

		List<DataType> projectedFields = actual.getProducedDataType().getChildren();
		assertEquals(2, projectedFields.size());
		assertEquals(projectedFields.get(0), DataTypes.INT());
		assertEquals(projectedFields.get(1), DataTypes.DOUBLE());
	}

	@Test
	public void testJDBCValidation() {
		// only password, no username
		try {
			Map<String, String> properties = getBasicProperties();
			properties.remove("connector.username");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			fail("exception expected");
		} catch (Exception ignored) {
		}

		// read partition properties not complete
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.read.partition.column", "aaa");
			properties.put("connector.read.partition.lower-bound", "-10");
			properties.put("connector.read.partition.upper-bound", "100");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			fail("exception expected");
		} catch (Exception ignored) {
		}

		// read partition lower-bound > upper-bound
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.read.partition.column", "aaa");
			properties.put("connector.read.partition.lower-bound", "100");
			properties.put("connector.read.partition.upper-bound", "-10");
			properties.put("connector.read.partition.num", "10");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			fail("exception expected");
		} catch (Exception ignored) {
		}

		// lookup cache properties not complete
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.lookup.cache.max-rows", "10");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			//fail("exception expected");
		} catch (Exception ignored) {
		}

		// lookup cache properties not complete
		try {
			Map<String, String> properties = getBasicProperties();
			properties.put("connector.lookup.cache.ttl", "1s");

			TableFactoryService.find(StreamTableSourceFactory.class, properties)
				.createStreamTableSource(properties);
			//fail("exception expected");
		} catch (Exception ignored) {
		}
	}

	private Map<String, String> getBasicProperties() {
		Map<String, String> properties = new HashMap<>();

		properties.put("connector.type", "jdbc");
		properties.put("connector.property-version", "1");

		properties.put("connector.driver", "org.apache.derby.jdbc.EmbeddedDriver");
		properties.put("connector.url", "jdbc:derby:memory:mydb");
		properties.put("connector.username", "user");
		properties.put("connector.password", "pass");
		properties.put("connector.table", "mytable");

		DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(properties);
		descriptorProperties.putTableSchema("schema", schema);

		return new HashMap<>(descriptorProperties.asMap());
	}
}
