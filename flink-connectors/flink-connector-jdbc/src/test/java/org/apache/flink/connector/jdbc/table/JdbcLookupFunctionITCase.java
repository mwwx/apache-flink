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
import org.apache.flink.connector.jdbc.JdbcTestBase;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.sources.lookup.LookupOptions;
import org.apache.flink.table.sources.lookup.cache.CacheStrategy;
import org.apache.flink.table.sources.lookup.cache.CacheType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.jdbc.JdbcTestBase.DRIVER_CLASS;

/**
 * IT case for {@link org.apache.flink.table.sources.lookup.SyncLookupTableFunction}.
 */
public class JdbcLookupFunctionITCase extends AbstractTestBase {

	public static final String DB_URL = "jdbc:derby:memory:lookup";
	public static final String LOOKUP_TABLE = "lookup_table";

	@Before
	public void before() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", JdbcTestBase.class.getCanonicalName() + ".DEV_NULL");

		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement()) {
			stat.executeUpdate("CREATE TABLE " + LOOKUP_TABLE + " (" +
				"id1 INT NOT NULL DEFAULT 0," +
				"id2 INT NOT NULL DEFAULT 0," +
				"comment1 VARCHAR(1000)," +
				"comment2 VARCHAR(1000))");

			Object[][] data = new Object[][]{
				new Object[]{1, 1, "11-c1-v1", "11-c2-v1"},
				new Object[]{1, 1, "11-c1-v2", "11-c2-v2"},
				new Object[]{2, 3, null, "23-c2"},
				new Object[]{2, 5, "25-c1", "25-c2"},
				new Object[]{3, 8, "38-c1", "38-c2"}
			};
			boolean[] surroundedByQuotes = new boolean[]{
				false, false, true, true
			};

			StringBuilder sqlQueryBuilder = new StringBuilder(
				"INSERT INTO " + LOOKUP_TABLE + " (id1, id2, comment1, comment2) VALUES ");
			for (int i = 0; i < data.length; i++) {
				sqlQueryBuilder.append("(");
				for (int j = 0; j < data[i].length; j++) {
					if (data[i][j] == null) {
						sqlQueryBuilder.append("null");
					} else {
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
						sqlQueryBuilder.append(data[i][j]);
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
					}
					if (j < data[i].length - 1) {
						sqlQueryBuilder.append(", ");
					}
				}
				sqlQueryBuilder.append(")");
				if (i < data.length - 1) {
					sqlQueryBuilder.append(", ");
				}
			}
			stat.execute(sqlQueryBuilder.toString());
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DRIVER_CLASS);
		try (
			Connection conn = DriverManager.getConnection(DB_URL);
			Statement stat = conn.createStatement()) {
			stat.execute("DROP TABLE " + LOOKUP_TABLE);
		}
	}

	@Test
	public void testSyncJDBCAllCache() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		StreamITCase.clear();

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, 1),
			new Tuple2<>(1, 1),
			new Tuple2<>(2, 3),
			new Tuple2<>(2, 5),
			new Tuple2<>(3, 5),
			new Tuple2<>(3, 8)
		)), "id1, id2");

		tEnv.registerTable("T", t);

		JdbcTableSource.Builder builder = JdbcTableSource.builder()
			.setOptions(JdbcOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(LOOKUP_TABLE)
				.build())
			.setSchema(TableSchema.builder().fields(
				new String[]{"id1", "id2", "comment1", "comment2"},
				new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()})
				.build());

		builder.setLookupOptions(LookupOptions.builder()
			.setCacheStrategy(CacheStrategy.ALL)
			.setCacheType(CacheType.MEMORY)
			.setMaxRetryTimes(3)
			.build());

		tEnv.registerFunction("jdbcLookup",
			builder.build().getLookupFunction(t.getSchema().getFieldNames()));

		String sqlQuery = "SELECT id1, id2, comment1, comment2 FROM T, " +
			"LATERAL TABLE(jdbcLookup(id1, id2)) AS S(l_id1, l_id2, comment1, comment2)";
		Table result = tEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("2,3,null,23-c2");
		expected.add("2,5,25-c1,25-c2");
		expected.add("3,8,38-c1,38-c2");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testSyncJDBCLruCache() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		StreamITCase.clear();

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, 1),
			new Tuple2<>(1, 1),
			new Tuple2<>(2, 3),
			new Tuple2<>(2, 5),
			new Tuple2<>(3, 5),
			new Tuple2<>(3, 8)
		)), "id1, id2");

		tEnv.registerTable("T", t);

		JdbcTableSource.Builder builder = JdbcTableSource.builder()
			.setOptions(JdbcOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(LOOKUP_TABLE)
				.build())
			.setSchema(TableSchema.builder().fields(
				new String[]{"id1", "id2", "comment1", "comment2"},
				new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()})
				.build());

		builder.setLookupOptions(LookupOptions.builder()
			.setCacheStrategy(CacheStrategy.LRU)
			.setCacheType(CacheType.MEMORY)
			.setMaxRetryTimes(3)
			.setCacheSize(1000)
			.setCacheTTLMs(1000 * 1000)
			.build());

		tEnv.registerFunction("jdbcLookup",
			builder.build().getLookupFunction(t.getSchema().getFieldNames()));

		String sqlQuery = "SELECT id1, id2, comment1, comment2 FROM T, " +
			"LATERAL TABLE(jdbcLookup(id1, id2)) AS S(l_id1, l_id2, comment1, comment2)";
		Table result = tEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("2,3,null,23-c2");
		expected.add("2,5,25-c1,25-c2");
		expected.add("3,8,38-c1,38-c2");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testSyncJDBCNoneCache() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		StreamITCase.clear();

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, 1),
			new Tuple2<>(1, 1),
			new Tuple2<>(2, 3),
			new Tuple2<>(2, 5),
			new Tuple2<>(3, 5),
			new Tuple2<>(3, 8)
		)), "id1, id2");

		tEnv.registerTable("T", t);

		JdbcTableSource.Builder builder = JdbcTableSource.builder()
			.setOptions(JdbcOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(LOOKUP_TABLE)
				.build())
			.setSchema(TableSchema.builder().fields(
				new String[]{"id1", "id2", "comment1", "comment2"},
				new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()})
				.build());

		builder.setLookupOptions(LookupOptions.builder()
			.setCacheStrategy(CacheStrategy.NONE)
			.setCacheType(CacheType.MEMORY)
			.setMaxRetryTimes(3)
			.build());

		tEnv.registerFunction("jdbcLookup",
			builder.build().getLookupFunction(t.getSchema().getFieldNames()));

		String sqlQuery = "SELECT id1, id2, comment1, comment2 FROM T, " +
			"LATERAL TABLE(jdbcLookup(id1, id2)) AS S(l_id1, l_id2, comment1, comment2)";
		Table result = tEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("2,3,null,23-c2");
		expected.add("2,5,25-c1,25-c2");
		expected.add("3,8,38-c1,38-c2");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testAsyncJDBCLruCache() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		StreamITCase.clear();

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, 1),
			new Tuple2<>(1, 1),
			new Tuple2<>(2, 3),
			new Tuple2<>(2, 5),
			new Tuple2<>(3, 5),
			new Tuple2<>(3, 8)
		)), "id1, id2");

		tEnv.registerTable("T", t);

		JdbcTableSource.Builder builder = JdbcTableSource.builder()
			.setOptions(JdbcOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(LOOKUP_TABLE)
				.build())
			.setSchema(TableSchema.builder().fields(
				new String[]{"id1", "id2", "comment1", "comment2"},
				new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()})
				.build());

		builder.setLookupOptions(LookupOptions.builder()
			.setCacheStrategy(CacheStrategy.LRU)
			.setCacheType(CacheType.MEMORY)
			.setMaxRetryTimes(3)
			.setCacheSize(1000)
			.setCacheTTLMs(1000 * 1000)
			.setAsyncEnabled(true)
			.build());

		tEnv.registerFunction("jdbcLookup",
			builder.build().getLookupFunction(t.getSchema().getFieldNames()));

		String sqlQuery = "SELECT id1, id2, comment1, comment2 FROM T, " +
			"LATERAL TABLE(jdbcLookup(id1, id2)) AS S(l_id1, l_id2, comment1, comment2)";
		Table result = tEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("2,3,null,23-c2");
		expected.add("2,5,25-c1,25-c2");
		expected.add("3,8,38-c1,38-c2");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testAsyncJDBCNoneCache() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		StreamITCase.clear();

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, 1),
			new Tuple2<>(1, 1),
			new Tuple2<>(2, 3),
			new Tuple2<>(2, 5),
			new Tuple2<>(3, 5),
			new Tuple2<>(3, 8)
		)), "id1, id2");

		tEnv.registerTable("T", t);

		JdbcTableSource.Builder builder = JdbcTableSource.builder()
			.setOptions(JdbcOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(LOOKUP_TABLE)
				.build())
			.setSchema(TableSchema.builder().fields(
				new String[]{"id1", "id2", "comment1", "comment2"},
				new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()})
				.build());

		builder.setLookupOptions(LookupOptions.builder()
			.setCacheStrategy(CacheStrategy.NONE)
			.setCacheType(CacheType.MEMORY)
			.setMaxRetryTimes(3)
			.setAsyncEnabled(true)
			.build());

		tEnv.registerFunction("jdbcLookup",
			builder.build().getLookupFunction(t.getSchema().getFieldNames()));

		String sqlQuery = "SELECT id1, id2, comment1, comment2 FROM T, " +
			"LATERAL TABLE(jdbcLookup(id1, id2)) AS S(l_id1, l_id2, comment1, comment2)";
		Table result = tEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("2,3,null,23-c2");
		expected.add("2,5,25-c1,25-c2");
		expected.add("3,8,38-c1,38-c2");

		StreamITCase.compareWithList(expected);
	}

}
