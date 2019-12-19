package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.setRecordToStatement;

/**
 * Retract writer to deal with insert, delete message.
 */
public class TableJdbcRetractOutputFormat extends JdbcBatchingOutputFormat<Tuple2<Boolean, Row>, Row, JdbcBatchStatementExecutor<Row>>{

	private static final Logger LOG = LoggerFactory.getLogger(TableJdbcRetractOutputFormat.class);

	private JdbcBatchStatementExecutor<Row> deleteExecutor;
	private final JdbcDmlOptions dmlOptions;

	public TableJdbcRetractOutputFormat(JdbcConnectionProvider connectionProvider, JdbcDmlOptions dmlOptions, JdbcExecutionOptions batchOptions) {
		super(connectionProvider, batchOptions, ctx -> createSimpleRowExecutor(dmlOptions, ctx), tuple2 -> tuple2.f1);
		this.dmlOptions = dmlOptions;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		deleteExecutor = createDeleteExecutor(getRuntimeContext());
		try {
			deleteExecutor.prepareStatements(connection);
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

	@Override
	protected void addToBatch(Tuple2<Boolean, Row> original, Row extracted) throws SQLException {
		if (original.f0) {
			super.addToBatch(original, extracted);
		} else {
			// if extracted has null value, first flush all record, then execute delete
			Map<String, Integer> nullValueFields = hasNullValue(extracted, dmlOptions.getFieldNames());
			if (nullValueFields.size() > 0) {
				// first flush cache
				attemptFlush();
				// execute delete
				executeDeleteWithEmptyValue(extracted, nullValueFields, connection);
			} else {
				deleteExecutor.addToBatch(extracted);
			}
		}
	}

	@Override
	public synchronized void close() {
		try {
			super.close();
		} finally {
			try {
				if (deleteExecutor != null){
					deleteExecutor.closeStatements();
				}
			} catch (SQLException e) {
				LOG.warn("unable to close delete statement runner", e);
			}
		}
	}

	@Override
	protected void attemptFlush() throws SQLException {
		super.attemptFlush();
		deleteExecutor.executeBatch();
	}

	private JdbcBatchStatementExecutor<Row> createDeleteExecutor(RuntimeContext ctx) {
		return JdbcBatchStatementExecutor.simple(
			dmlOptions.getDialect().getDeleteStatement(dmlOptions.getTableName(), dmlOptions.getFieldNames()),
			(st, record) -> setRecordToStatement(st, dmlOptions.getFieldTypes(), record),
			ctx.getExecutionConfig().isObjectReuseEnabled() ? Row::copy : Function.identity());
	}

	private static JdbcBatchStatementExecutor<Row> createSimpleRowExecutor(JdbcDmlOptions opt, RuntimeContext ctx) {
		return JdbcBatchStatementExecutor.simple(
			opt.getDialect().getInsertIntoStatement(opt.getTableName(), opt.getFieldNames()),
			(st, record) -> setRecordToStatement(st, opt.getFieldTypes(), record),
			ctx.getExecutionConfig().isObjectReuseEnabled() ? Row::copy : Function.identity());
	}

	private Map<String, Integer> hasNullValue(Row extracted, String[] fieldNames) {
		Map<String, Integer> nullValueMap = new HashMap<>();

		for (int index = 0; index < extracted.getArity(); index++) {
			if (extracted.getField(index) == null) {
				nullValueMap.put(fieldNames[index], index);
			}
		}

		return nullValueMap;
	}

	private void executeDeleteWithEmptyValue(
		Row extracted,
		Map<String, Integer> nullValueFields,
		Connection connection) throws SQLException {
		String deleteSql = dmlOptions
			.getDialect()
			.getDeleteStatement(
				dmlOptions.getTableName(),
				dmlOptions.getFieldNames(),
				nullValueFields);

		PreparedStatement deleteNullStatement = null;

		try {
			deleteNullStatement = connection.prepareStatement(deleteSql);
			JdbcUtils.setRecordToStatement(
				deleteNullStatement,
				dmlOptions.getFieldTypes(),
				extracted,
				true);
			deleteNullStatement.executeUpdate();
		} finally {
			if (deleteNullStatement != null) {
				deleteNullStatement.close();
			}
		}
	}
}
