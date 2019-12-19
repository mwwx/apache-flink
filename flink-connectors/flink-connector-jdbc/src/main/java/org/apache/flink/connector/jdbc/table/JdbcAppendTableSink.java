package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.AbstractJdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An at-least-once Table sink for JDBC.
 *
 * <p>The mechanisms of Flink guarantees delivering messages at-least-once to this sink (if
 * checkpointing is enabled). However, one common use case is to run idempotent queries
 * (e.g., <code>REPLACE</code> or <code>INSERT OVERWRITE</code>) to upsert into the database and
 * achieve exactly-once semantic.</p>
 */
public class JdbcAppendTableSink implements AppendStreamTableSink<Row> {

	private final TableSchema schema;
	private final JdbcOptions options;
	private final int flushMaxSize;
	private final long flushIntervalMills;
	private final int maxRetryTime;

	@VisibleForTesting
	private AbstractJdbcOutputFormat<Row> outputFormat;

	JdbcAppendTableSink(
		TableSchema schema,
		JdbcOptions options,
		int flushMaxSize,
		long flushIntervalMills,
		int maxRetryTime) {
		this.schema = schema;
		this.options = options;
		this.flushMaxSize = flushMaxSize;
		this.flushIntervalMills = flushIntervalMills;
		this.maxRetryTime = maxRetryTime;
	}

	public static Builder builder() {
		return new Builder();
	}

	private JdbcBatchingOutputFormat<Row, Row, JdbcBatchStatementExecutor<Row>> newFormat() {
		// sql types
		int[] jdbcSqlTypes = Arrays.stream(schema.getFieldTypes())
			.mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();

		return JdbcBatchingOutputFormat.builder()
			.setOptions(options)
			.setFieldNames(schema.getFieldNames())
			.setFlushMaxSize(flushMaxSize)
			.setFlushIntervalMills(flushIntervalMills)
			.setMaxRetryTimes(maxRetryTime)
			.setFieldTypes(jdbcSqlTypes)
			.buildAppend();
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		this.outputFormat = newFormat();
		return dataStream
			.addSink(new GenericJdbcSinkFunction<>(this.outputFormat))
			.setParallelism(dataStream.getParallelism())
			.name(TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames()));
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());
	}

	@Override
	public String[] getFieldNames() {
		return schema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return schema.getFieldTypes();
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
				"Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
				"But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		}

		return new JdbcAppendTableSink(
			schema, options, flushMaxSize, flushIntervalMills, maxRetryTime);
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@VisibleForTesting
	protected AbstractJdbcOutputFormat<Row> getOutputFormat() {
		return this.outputFormat;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JdbcAppendTableSink that = (JdbcAppendTableSink) o;
		return flushMaxSize == that.flushMaxSize &&
			flushIntervalMills == that.flushIntervalMills &&
			maxRetryTime == that.maxRetryTime &&
			Objects.equals(schema, that.schema) &&
			Objects.equals(options, that.options);
	}

	@Override
	public int hashCode() {
		return Objects.hash(schema, options, flushMaxSize, flushIntervalMills, maxRetryTime);
	}

	/**
	 * A builder to configure and build the JDBCAppendTableSink.
	 */
	public static class Builder {
		private TableSchema schema;
		private JdbcOptions options;
		private int flushMaxSize = AbstractJdbcOutputFormat.DEFAULT_FLUSH_MAX_SIZE;
		private long flushIntervalMills = AbstractJdbcOutputFormat.DEFAULT_FLUSH_INTERVAL_MILLS;
		private int maxRetryTimes = JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES;

		/**
		 * required, table schema of this table source.
		 */
		public Builder setTableSchema(TableSchema schema) {
			this.schema = JdbcTypeUtil.normalizeTableSchema(schema);
			return this;
		}

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JdbcOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, flush max size (includes all append, upsert and delete records),
		 * over this number of records, will flush data.
		 */
		public Builder setFlushMaxSize(int flushMaxSize) {
			this.flushMaxSize = flushMaxSize;
			return this;
		}

		/**
		 * optional, flush interval mills, over this time, asynchronous threads will flush data.
		 */
		public Builder setFlushIntervalMills(long flushIntervalMills) {
			this.flushIntervalMills = flushIntervalMills;
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCOutputFormat
		 */
		public JdbcAppendTableSink build() {
			checkNotNull(schema, "No schema supplied.");
			checkNotNull(options, "No options supplied.");

			return new JdbcAppendTableSink(schema, options, flushMaxSize, flushIntervalMills, maxRetryTimes);
		}
	}
}
