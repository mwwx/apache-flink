package org.apache.flink.table.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.StreamITCase;
import org.apache.flink.types.Row;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * CSVLookupFunctionTest.
 */
public class CSVLookupFunctionTest {

	@Test
	@Ignore
	public void testCSVCacheAll() throws Exception {
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

		CsvTableSource.Builder builder = CsvTableSource.builder()
			.fields(new String[]{"id1", "id2", "comment1", "comment2"},
				new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()})
			.path(CSVLookupFunctionTest.class.getClassLoader().getResource("csvLookupData.csv").getFile());

		tEnv.registerFunction("csvLookup",
			builder.build().getLookupFunction(t.getSchema().getFieldNames()));

		String sqlQuery = "SELECT id1, id2, comment1, comment2 FROM T, " +
			"LATERAL TABLE(csvLookup(id1, id2)) AS S(l_id1, l_id2, comment1, comment2)";
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
