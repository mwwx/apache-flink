package org.apache.flink.connector.jdbc.internal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.sources.lookup.DataFetcher;
import org.apache.flink.table.sources.lookup.LookupOptions;
import org.apache.flink.table.sources.lookup.cache.CacheStrategy;
import org.apache.flink.types.Row;

import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.jdbc.utils.JdbcUtils.getFieldFromResultSet;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * JdbcDataFetcher.
 */
public class JdbcDataFetcher implements DataFetcher {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcDataFetcher.class);

	private final String query;
	private final JdbcOptions options;
	private final TypeInformation<?>[] keyTypes;
	private final int[] keySqlTypes;
	private final String[] fieldNames;
	private final TypeInformation<?>[] fieldTypes;
	private final int[] outputSqlTypes;

	private transient Connection dbConn;
	private transient PreparedStatement queryStatement;

	private int[] keyIndex;

	private int fetchSize;

	public JdbcDataFetcher(
		JdbcOptions options,
		LookupOptions lookupOptions,
		String[] fieldNames,
		TypeInformation<?>[] fieldTypes,
		String[] keyNames,
		int fetchSize) {
		this.options = options;
		this.fetchSize = fetchSize;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;

		List<String> nameList = Arrays.asList(fieldNames);
		this.keyTypes = Arrays.stream(keyNames)
			.map(s -> {
				checkArgument(nameList.contains(s),
					"keyName %s can't find in fieldNames %s.", s, nameList);
				return fieldTypes[nameList.indexOf(s)];
			})
			.toArray(TypeInformation[]::new);
		this.keySqlTypes = Arrays.stream(keyTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();

		List<String> fieldNameList = new ArrayList<>();
		Collections.addAll(fieldNameList, fieldNames);
		this.keyIndex = new int[keyNames.length];
		for (int i = 0; i < keyNames.length; i++) {
			keyIndex[i] = fieldNameList.indexOf(keyNames[i]);
		}

		this.outputSqlTypes = Arrays.stream(fieldTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
		if (CacheStrategy.ALL.equals(lookupOptions.getCacheStrategy())) {
			this.query = options.getDialect().getSelectAllFromStatement(
				options.getTableName(), fieldNames);
		} else {
			this.query = options.getDialect().getSelectFromStatement(
				options.getTableName(), fieldNames, keyNames);
		}
	}

	@Override
	public void open() {
		try {
			establishConnection();
			queryStatement = dbConn.prepareStatement(query);
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}
	}

	private Row convertToRowFromResultSet(ResultSet resultSet) throws SQLException {
		Row row = new Row(outputSqlTypes.length);
		for (int i = 0; i < outputSqlTypes.length; i++) {
			row.setField(i, getFieldFromResultSet(i, outputSqlTypes[i], resultSet, fieldTypes[i]));
		}
		return row;
	}

	private void establishConnection() throws SQLException, ClassNotFoundException {
		Class.forName(options.getDriverName());

		if (!options.getUsername().isPresent()) {
			dbConn = DriverManager.getConnection(options.getDbURL());
		} else {
			dbConn = DriverManager.getConnection(options.getDbURL(),
				options.getUsername().get(), options.getPassword().orElse(null));
		}
	}

	@Override
	public void loadAllData(HTreeMap<Row, List<Row>> cache) throws Exception {
		JdbcInputFormat.JdbcInputFormatBuilder inputFormatBuilder = JdbcInputFormat
			.buildJdbcInputFormat()
			.setDrivername(options.getDriverName())
			.setUsername(options.getUsername().orElse(null))
			.setPassword(options.getPassword().orElse(null))
			.setQuery(query)
			.setRowTypeInfo(getResultType())
			.setFetchSize(fetchSize);

		if (JdbcUtils.isLinkoopdb(options.getDriverName())) {
			//不加;query_iterator=1参数select只会返回100条数据
			inputFormatBuilder.setDBUrl(options.getDbURL() + ";query_iterator=1");
		} else {
			inputFormatBuilder.setDBUrl(options.getDbURL());
		}
		JdbcInputFormat inputFormat = inputFormatBuilder.finish();

		inputFormat.openInputFormat();
		InputSplit[] inputSplits = inputFormat.createInputSplits(1);
		Row row = new Row(getResultType().getArity());
		for (InputSplit inputSplit : inputSplits) {
			inputFormat.open(inputSplit);
			while (true) {
				Row value = inputFormat.nextRecord(row);
				if (value == null) {
					break;
				} else {
					Row key = Row.project(value, keyIndex);
					List<Row> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
					rows.add(Row.copy(value));
					cache.put(key, rows);
				}
			}
			inputFormat.close();
		}
		inputFormat.closeInputFormat();
	}

	@Override
	public List<Row> syncRequestByKey(Row key) throws Exception {
		queryStatement.clearParameters();
		try {
			queryStatement.setFetchSize(Integer.MIN_VALUE);
		} catch (SQLException se) {
			LOG.warn("set fetchSize {} failed.", Integer.MIN_VALUE);
		}

		for (int i = 0; i < key.getArity(); i++) {
			JdbcUtils.setField(queryStatement, keySqlTypes[i], key.getField(i), i);
		}
		ArrayList<Row> rows = new ArrayList<>();
		try (ResultSet resultSet = queryStatement.executeQuery()) {
			while (resultSet.next()) {
				Row row = convertToRowFromResultSet(resultSet);
				rows.add(row);
			}
		}
		rows.trimToSize();
		return rows;
	}

	@Override
	public boolean supportAsync() {
		return true;
	}

	@Override
	public RowTypeInfo getResultType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return keyTypes;
	}

	@Override
	public void close() throws Exception {
		if (queryStatement != null) {
			try {
				queryStatement.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				queryStatement = null;
			}
		}

		if (dbConn != null) {
			try {
				dbConn.close();
			} catch (SQLException se) {
				LOG.info("JDBC connection could not be closed: " + se.getMessage());
			} finally {
				dbConn = null;
			}
		}
	}
}
