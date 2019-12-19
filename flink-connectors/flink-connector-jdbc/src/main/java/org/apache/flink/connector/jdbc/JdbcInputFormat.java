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

package org.apache.flink.connector.jdbc;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * InputFormat to read data from a database and generate Rows.
 * The InputFormat has to be configured using the supplied InputFormatBuilder.
 * A valid RowTypeInfo must be properly configured in the builder, e.g.:
 *
 * <pre><code>
 * TypeInformation<?>[] fieldTypes = new TypeInformation<?>[] {
 *		BasicTypeInfo.INT_TYPE_INFO,
 *		BasicTypeInfo.STRING_TYPE_INFO,
 *		BasicTypeInfo.STRING_TYPE_INFO,
 *		BasicTypeInfo.DOUBLE_TYPE_INFO,
 *		BasicTypeInfo.INT_TYPE_INFO
 *	};
 *
 * RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
 *
 * JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
 *				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
 *				.setDBUrl("jdbc:derby:memory:ebookshop")
 *				.setQuery("select * from books")
 *				.setRowTypeInfo(rowTypeInfo)
 *				.finish();
 * </code></pre>
 *
 * <p>In order to query the JDBC source in parallel, you need to provide a
 * parameterized query template (i.e. a valid {@link PreparedStatement}) and
 * a {@link JdbcParameterValuesProvider} which provides binding values for the
 * query parameters. E.g.:
 *
 * <pre><code>
 *
 * Serializable[][] queryParameters = new String[2][1];
 * queryParameters[0] = new String[]{"Kumar"};
 * queryParameters[1] = new String[]{"Tan Ah Teck"};
 *
 * JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
 *				.setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
 *				.setDBUrl("jdbc:derby:memory:ebookshop")
 *				.setQuery("select * from books WHERE author = ?")
 *				.setRowTypeInfo(rowTypeInfo)
 *				.setParametersProvider(new JdbcGenericParameterValuesProvider(queryParameters))
 *				.finish();
 * </code></pre>
 *
 * @see Row
 * @see JdbcParameterValuesProvider
 * @see PreparedStatement
 * @see DriverManager
 */
@Experimental
public class JdbcInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

	protected static final long serialVersionUID = 1L;
	protected static final Logger LOG = LoggerFactory.getLogger(JdbcInputFormat.class);

	protected final JdbcConnectionOptions jdbcOptions;
	protected transient JdbcConnectionProvider connectionProvider;

	protected String queryTemplate;
	protected int resultSetType;
	protected int resultSetConcurrency;
	protected RowTypeInfo rowTypeInfo;

	protected transient Connection dbConn;
	protected transient PreparedStatement statement;
	protected transient ResultSet resultSet;
	protected int fetchSize;
	// Boolean to distinguish between default value and explicitly set autoCommit mode.
	protected Boolean autoCommit;

	protected boolean hasNext;
	protected Object[][] parameterValues;

	protected String increaseColumn;
	private int increaseColumnIdx = -1;
	private volatile long currentPos = Long.MIN_VALUE;
	protected int[] parameterTypes;

	public JdbcInputFormat(JdbcConnectionOptions jdbcOptions) {
		this.jdbcOptions = jdbcOptions;
	}

	@Override
	public RowTypeInfo getProducedType() {
		return rowTypeInfo;
	}

	@Override
	public void configure(Configuration parameters) {
		//do nothing here
	}

	@Override
	public void openInputFormat() {
		//called once per inputFormat (on open)
		try {
			this.connectionProvider = new SimpleJdbcConnectionProvider(jdbcOptions);
			dbConn = connectionProvider.getConnection();

			// set autoCommit mode only if it was explicitly configured.
			// keep connection default otherwise.
			if (autoCommit != null) {
				dbConn.setAutoCommit(autoCommit);
			}

			if (increaseColumn != null && !increaseColumn.isEmpty()) {
				increaseColumnIdx = rowTypeInfo.getFieldIndex(increaseColumn);
				if (increaseColumnIdx == -1) {
					throw new IllegalArgumentException(
						String.format("column %s was not a member of schema.", increaseColumn));
				}

				String query = queryTemplate + String.format(" where %s > ? order by %s", increaseColumn, increaseColumn);
				statement = dbConn.prepareStatement(query, resultSetType, resultSetConcurrency);
				statement.setObject(1, currentPos);
			} else {
				statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
			}

			if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
				statement.setFetchSize(fetchSize);
			}

			LOG.info("JdbcInputFormat openInputFormat at increaseColumn {}, {} successful.", increaseColumn, fetchSize);
		} catch (SQLException se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		} catch (Exception ex) {
			if (ex instanceof ClassNotFoundException) {
				throw new IllegalArgumentException("JDBC-Class not found. - " + ex.getMessage(), ex);
			}
			throw new IllegalArgumentException("open() failed." + ex.getMessage(), ex);
		}
	}

	@Override
	public void closeInputFormat() {
		//called once per inputFormat (on close)
		try {
			if (statement != null) {
				statement.close();
			}
		} catch (SQLException se) {
			LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
		} finally {
			statement = null;
		}

		try {
			if (dbConn != null) {
				dbConn.close();
			}
		} catch (SQLException se) {
			LOG.info("Inputformat couldn't be closed - " + se.getMessage());
		} finally {
			dbConn = null;
		}

		if (connectionProvider != null) {
			connectionProvider.releaseConnectionPool();
			connectionProvider = null;
		}
		parameterValues = null;
	}

	/**
	 * Connects to the source database and executes the query in a <b>parallel
	 * fashion</b> if
	 * this {@link InputFormat} is built using a parameterized query (i.e. using
	 * a {@link PreparedStatement})
	 * and a proper {@link JdbcParameterValuesProvider}, in a <b>non-parallel
	 * fashion</b> otherwise.
	 *
	 * @param inputSplit which is ignored if this InputFormat is executed as a
	 *        non-parallel source,
	 *        a "hook" to the query parameters otherwise (using its
	 *        <i>splitNumber</i>)
	 * @throws IOException if there's an error during the execution of the query
	 */
	@Override
	public void open(InputSplit inputSplit) throws IOException {
		try {
			if (inputSplit != null && parameterValues != null) {
				for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
					Object param = parameterValues[inputSplit.getSplitNumber()][i];
					if (param instanceof String) {
						statement.setString(i + 1, (String) param);
					} else if (param instanceof Long) {
						statement.setLong(i + 1, (Long) param);
					} else if (param instanceof Integer) {
						statement.setInt(i + 1, (Integer) param);
					} else if (param instanceof Double) {
						statement.setDouble(i + 1, (Double) param);
					} else if (param instanceof Boolean) {
						statement.setBoolean(i + 1, (Boolean) param);
					} else if (param instanceof Float) {
						statement.setFloat(i + 1, (Float) param);
					} else if (param instanceof BigDecimal) {
						statement.setBigDecimal(i + 1, (BigDecimal) param);
					} else if (param instanceof Byte) {
						statement.setByte(i + 1, (Byte) param);
					} else if (param instanceof Short) {
						statement.setShort(i + 1, (Short) param);
					} else if (param instanceof Date) {
						statement.setDate(i + 1, (Date) param);
					} else if (param instanceof Time) {
						statement.setTime(i + 1, (Time) param);
					} else if (param instanceof Timestamp) {
						statement.setTimestamp(i + 1, (Timestamp) param);
					} else if (param instanceof Array) {
						statement.setArray(i + 1, (Array) param);
					} else {
						//extends with other types if needed
						throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet).");
					}
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Executing '%s' with parameters %s", queryTemplate, Arrays.deepToString(parameterValues[inputSplit.getSplitNumber()])));
				}
			}
			resultSet = statement.executeQuery();
			hasNext = resultSet.next();
		} catch (SQLException se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		}
	}

	/**
	 * Closes all resources used.
	 *
	 * @throws IOException Indicates that a resource could not be closed.
	 */
	@Override
	public void close() throws IOException {
		if (resultSet == null) {
			return;
		}
		try {
			resultSet.close();
		} catch (SQLException se) {
			LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
		}
	}

	/**
	 * Checks whether all data has been read.
	 *
	 * @return boolean value indication whether all data has been read.
	 * @throws IOException
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		return !hasNext;
	}

	/**
	 * Stores the next resultSet row in a tuple.
	 *
	 * @param reuse row to be reused.
	 * @return row containing next {@link Row}
	 * @throws IOException
	 */
	@Override
	public Row nextRecord(Row reuse) throws IOException {
		try {
			if (!hasNext) {
				if (increaseColumnIdx != -1) {
					resultSet.close();
					statement.setObject(1, currentPos);
					resultSet = statement.executeQuery();
					hasNext = resultSet.next();
				}
				return null;
			}

			for (int pos = 0; pos < reuse.getArity(); pos++) {
				reuse.setField(pos, JdbcUtils.getFieldFromResultSet(
					pos, parameterTypes[pos], resultSet, rowTypeInfo.getTypeAt(pos)));

				if (pos == increaseColumnIdx) {
					currentPos = resultSet.getInt(increaseColumn);
				}
			}
			//update hasNext after we've read the record
			hasNext = resultSet.next();
			return reuse;
		} catch (SQLException se) {
			throw new IOException("Couldn't read data - " + se.getMessage(), se);
		} catch (NullPointerException npe) {
			throw new IOException("Couldn't access resultSet", npe);
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (parameterValues == null) {
			return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
		}
		GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = new GenericInputSplit(i, ret.length);
		}
		return ret;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@VisibleForTesting
	public PreparedStatement getStatement() {
		return statement;
	}

	@VisibleForTesting
	public Connection getDbConn() {
		return dbConn;
	}

	public String getIncreaseColumn() {
		return increaseColumn;
	}

	public void setCurrentPos(long currentPos) {
		this.currentPos = currentPos;
	}

	public long getCurrentPos() {
		return currentPos;
	}

	/**
	 * A builder used to set parameters to the output format's configuration in a fluent way.
	 * @return builder
	 */
	public static JdbcInputFormatBuilder buildJdbcInputFormat() {
		return new JdbcInputFormatBuilder();
	}

	/**
	 * Builder for {@link JdbcInputFormat}.
	 */
	public static class JdbcInputFormatBuilder {

		private String username;
		private String password;
		private String drivername;
		private String dbURL;
		private String queryTemplate;
		//using TYPE_FORWARD_ONLY for high performance reads
		private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
		private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
		private Object[][] parameterValues;
		private RowTypeInfo rowTypeInfo;
		private int fetchSize;
		private Boolean autoCommit;
		private String increaseColumn;

		public JdbcInputFormatBuilder() {
		}

		public JdbcInputFormatBuilder setUsername(String username) {
			this.username = username;
			return this;
		}

		public JdbcInputFormatBuilder setPassword(String password) {
			this.password = password;
			return this;
		}

		public JdbcInputFormatBuilder setDrivername(String drivername) {
			this.drivername = drivername;
			return this;
		}

		public JdbcInputFormatBuilder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		public JdbcInputFormatBuilder setQuery(String query) {
			this.queryTemplate = query;
			return this;
		}

		public JdbcInputFormatBuilder setResultSetType(int resultSetType) {
			this.resultSetType = resultSetType;
			return this;
		}

		public JdbcInputFormatBuilder setResultSetConcurrency(int resultSetConcurrency) {
			this.resultSetConcurrency = resultSetConcurrency;
			return this;
		}

		public JdbcInputFormatBuilder setParametersProvider(JdbcParameterValuesProvider parameterValuesProvider) {
			this.parameterValues = parameterValuesProvider.getParameterValues();
			return this;
		}

		public JdbcInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
			this.rowTypeInfo = rowTypeInfo;
			return this;
		}

		public JdbcInputFormatBuilder setFetchSize(int fetchSize) {
			Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize >= 0,
				"Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
			this.fetchSize = fetchSize;
			return this;
		}

		public JdbcInputFormatBuilder setAutoCommit(Boolean autoCommit) {
			this.autoCommit = autoCommit;
			return this;
		}

		public JdbcInputFormatBuilder setIncreaseColumn(String increaseColumn) {
			this.increaseColumn = increaseColumn;
			return this;
		}

		public JdbcInputFormat finish() {
			if (this.dbURL == null) {
				throw new IllegalArgumentException("No database URL supplied");
			}
			if (this.queryTemplate == null) {
				throw new IllegalArgumentException("No query supplied");
			}
			if (this.drivername == null) {
				throw new IllegalArgumentException("No driver supplied");
			}

			JdbcConnectionOptions options = new JdbcConnectionOptions
				.JdbcConnectionOptionsBuilder()
				.withUrl(this.dbURL)
				.withDriverName(this.drivername)
				.withUsername(this.username)
				.withPassword(this.password)
				.build();

			if (this.rowTypeInfo == null) {
				throw new IllegalArgumentException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
			}
			if (this.parameterValues == null) {
				LOG.debug("No input splitting configured (data will be read with parallelism 1).");
			}

			final JdbcInputFormat format = new JdbcInputFormat(options);
			format.queryTemplate = this.queryTemplate;
			format.resultSetType = this.resultSetType;
			format.resultSetConcurrency = this.resultSetConcurrency;
			format.parameterValues = this.parameterValues;
			format.rowTypeInfo = this.rowTypeInfo;
			format.fetchSize = this.fetchSize;
			format.autoCommit = this.autoCommit;
			format.increaseColumn = this.increaseColumn;
			// sql types
			format.parameterTypes = Arrays.stream(format.rowTypeInfo.getFieldTypes())
				.mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();

			return format;
		}

	}

}
