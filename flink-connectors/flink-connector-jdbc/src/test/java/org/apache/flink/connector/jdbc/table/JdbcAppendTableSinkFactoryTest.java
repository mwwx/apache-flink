package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * JDBCAppendTableSinkFactoryTest.
 */
public class JdbcAppendTableSinkFactoryTest {

	private static final TableSchema schema = TableSchema.builder()
		.field("aaa", DataTypes.INT())
		.field("bbb", DataTypes.STRING())
		.field("ccc", DataTypes.DOUBLE())
		.field("ddd", DataTypes.DECIMAL(24, 3))
		.field("eee", DataTypes.TIMESTAMP(3))
		.build();

	@Test
	public void testJDBCSinkProperties() {
		Map<String, String> properties = getBasicProperties();
		properties.put("update-mode", "append");
		properties.put("connector.write.flush.max-rows", "1");

		final StreamTableSink<?> actual = TableFactoryService
			.find(StreamTableSinkFactory.class, properties)
			.createStreamTableSink(properties);

		final JdbcAppendTableSink.Builder builder = JdbcAppendTableSink.builder();

		JdbcOptions options = JdbcOptions
			.builder()
			.setDriverName("org.apache.derby.jdbc.EmbeddedDriver")
			.setDBUrl("jdbc:derby:memory:mydb")
			.setUsername("user")
			.setPassword("pass")
			.setTableName("mytable")
			.setDialect(JdbcDialects.get("jdbc:derby:memory:mydb").get())
			.build();

		JdbcAppendTableSink expected = builder
			.setOptions(options)
			.setTableSchema(actual.getTableSchema())
			.setFlushMaxSize(1)
			/*.setQuery("INSERT INTO mytable (aaa,bbb,ccc,ddd,eee) VALUES (?,?,?,?,?)")
			.setParameterTypes(4, 12, 8, 3, 93)*/
			.build();

		Assert.assertEquals(expected, actual);
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
