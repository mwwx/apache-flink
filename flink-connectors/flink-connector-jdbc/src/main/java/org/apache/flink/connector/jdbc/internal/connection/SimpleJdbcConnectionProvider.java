/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Simple JDBC connection provider.
 */
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

	private static final long serialVersionUID = 1L;

	private final JdbcConnectionOptions jdbcOptions;

	private transient volatile Connection connection;

	private transient volatile BasicDataSource dataSource;

	public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
		this.jdbcOptions = jdbcOptions;
	}

	@Override
	public Connection getConnection() throws SQLException, ClassNotFoundException {
		if (dataSource == null) {
			synchronized (this) {
				if (dataSource == null) {
					dataSource = new BasicDataSource();
					dataSource.setDriverClassName(jdbcOptions.getDriverName());
					dataSource.setUrl(jdbcOptions.getDbURL());

					if (jdbcOptions.getUsername().isPresent()) {
						dataSource.setUsername(jdbcOptions.getUsername().get());
						dataSource.setPassword(jdbcOptions.getPassword().orElse(null));
					}

					dataSource.setTestOnCreate(true);
					dataSource.setTestOnBorrow(true);
					dataSource.setTestOnReturn(true);
					dataSource.setTestWhileIdle(true);
					dataSource.setMaxTotal(2);
				}
			}
		}

		if (connection == null) {
			synchronized (this) {
				if (connection == null) {
					connection = dataSource.getConnection();
				}
			}
		}

		if (JdbcUtils.isLinkoopdb(jdbcOptions.getDriverName())) {
			Statement statement = connection.createStatement();
			statement.execute("set session count off");
			statement.close();
		}
		return connection;
	}

	@Override
	public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
		try {
			connection.close();
		} catch (SQLException ex) {
			LOG.warn("JDBC connection close failed.", ex);
		} finally {
			connection = null;
		}
		connection = getConnection();
		return connection;
	}

	@Override
	public void releaseConnectionPool() {
		try {
			if (dataSource != null) {
				dataSource.close();
			}
		} catch (SQLException ex) {
			LOG.warn("DataSource couldn't be closed.", ex);
		} finally {
			dataSource = null;
		}
	}
}
