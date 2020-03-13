package org.apache.flink.table.api.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * UnresolvedTableFunctionSqlVisitorTest.
 */
public class UnresolvedTableFunctionSqlVisitorTest {

	private static final List<String> results = new ArrayList<>();

	private static final EnvironmentSettings fsSettings = EnvironmentSettings
		.newInstance()
		.useBlinkPlanner()
		.inStreamingMode()
		.build();

	@Test
	public void testSelect() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

		List<Integer> data = new ArrayList<>();
		data.add(1);

		Table source = tEnv.fromDataStream(env.fromCollection(data));

		//table
		tEnv.registerTable("test", source);
		//function
		tEnv.registerFunction("fun", new TestUserDefinedTableFunction());

		//sql
		Table table = tEnv.sqlQuery("select * from test, lateral table(fun(1)) as T(str)");

		//to stream
		tEnv.toAppendStream(table, Row.class).addSink(new TestSinkFunction());

		results.clear();
		env.execute("testSelect");

		List<String> expected = new ArrayList<>();
		expected.add("1,1");

		Assert.assertEquals(expected, results);
	}

	@Test
	public void testInsert() throws Exception {
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment env = StreamTableEnvironment.create(streamExecutionEnvironment, fsSettings);

		List<Integer> data = new ArrayList<>();
		data.add(1);

		//table
		Table source = env.fromDataStream(streamExecutionEnvironment.fromCollection(data));
		env.registerTable("test", source);

		//function
		env.registerFunction("fun", new TestUserDefinedTableFunction());

		env.registerTableSink("sink", new AppendStreamTableSink<Row>() {
			@Override
			public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
				return this;
			}

			@Override
			public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
				return dataStream.addSink(new TestSinkFunction());
			}

			@Override
			public TableSchema getTableSchema() {
				return new TableSchema(new String[]{"a", "str"}, new TypeInformation[]{Types.INT, Types.STRING});
			}

			@Override
			public TypeInformation<Row> getOutputType() {
				return new RowTypeInfo(new TypeInformation[]{Types.INT, Types.STRING}, new String[]{"a", "str"});
			}
		});

		//sql
		env.sqlUpdate("insert into sink select * from test, lateral table(fun(1)) as T(str)");

		results.clear();

		//to stream
		env.execute("testInsert");

		List<String> expected = new ArrayList<>();
		expected.add("1,1");

		Assert.assertEquals(expected, results);
	}

	/**
	 * TestSinkFunction.
	 */
	class TestSinkFunction extends RichSinkFunction<Row> {

		@Override
		public void invoke(Row value, Context context) throws Exception {
			results.add(value.toString());
		}
	}

}
