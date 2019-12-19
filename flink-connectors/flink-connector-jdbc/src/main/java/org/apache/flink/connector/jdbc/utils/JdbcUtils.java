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

package org.apache.flink.connector.jdbc.utils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.dialect.CommonDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.BYTE_TYPE_INFO;
import static org.apache.flink.connector.jdbc.utils.JdbcTypeUtil.SIMPLE_TIMESTAMP_FORMAT;
import static org.apache.flink.connector.jdbc.utils.JdbcTypeUtil.SIMPLE_TIME_FORMAT;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_DRIVER;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_QUOTING;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_URL;
import static org.apache.flink.table.descriptors.JdbcValidator.CONNECTOR_USERNAME;

/**
 * Utils for jdbc connectors.
 */
public class JdbcUtils {

	private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);

	/**
	 * Adds a record to the prepared statement.
	 *
	 * <p>When this method is called, the output format is guaranteed to be opened.
	 *
	 * <p>WARNING: this may fail when no column types specified (because a best effort approach is attempted in order to
	 * insert a null value but it's not guaranteed that the JDBC driver handles PreparedStatement.setObject(pos, null))
	 *
	 * @param upload The prepared statement.
	 * @param typesArray The jdbc types of the row.
	 * @param row The records to add to the output.
	 * @see PreparedStatement
	 */
	public static void setRecordToStatement(PreparedStatement upload, int[] typesArray, Row row) throws SQLException {
		setRecordToStatement(upload, typesArray, row, false);
	}

	/**
	 * Adds a record to the prepared statement.
	 *
	 * <p>When this method is called, the output format is guaranteed to be opened.
	 *
	 * <p>WARNING: this may fail when no column types specified (because a best effort approach is attempted in order to
	 * insert a null value but it's not guaranteed that the JDBC driver handles PreparedStatement.setObject(pos, null))
	 *
	 * @param upload The prepared statement.
	 * @param typesArray The jdbc types of the row.
	 * @param row The records to add to the output.
	 * @param skipNull The flag whether skip set null field.
	 * @see PreparedStatement
	 */
	public static void setRecordToStatement(
		PreparedStatement upload, int[] typesArray, Row row, boolean skipNull) throws SQLException {
		if (typesArray != null && typesArray.length > 0 && typesArray.length != row.getArity()) {
			LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
		}

		int paramIndex = 0;
		if (typesArray == null) {
			// no types provided
			for (int index = 0; index < row.getArity(); index++) {
				if (skipNull && row.getField(index) == null) {
					continue;
				}

				paramIndex++;
				LOG.warn("Unknown column type for column {}. Best effort approach to set its value: {}.",
					paramIndex, row.getField(index));
				upload.setObject(paramIndex, row.getField(index));
			}
		} else {
			// types provided
			for (int i = 0; i < row.getArity(); i++) {
				setField(upload, typesArray[i], row.getField(i), paramIndex, skipNull);

				if (!skipNull || row.getField(i) != null) {
					paramIndex++;
				}
			}
		}
	}

	public static void setField(
		PreparedStatement upload,
		int type,
		Object field,
		int index) throws SQLException {
		setField(upload, type, field, index, false);
	}

	public static void setField(
		PreparedStatement upload,
		int type,
		Object field,
		int index,
		boolean skipNull) throws SQLException {
		if (field == null) {
			if (!skipNull) {
				upload.setNull(index + 1, type);
			}
		} else {
			try {
				// casting values as suggested by http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
				switch (type) {
					case java.sql.Types.NULL:
						upload.setNull(index + 1, type);
						break;
					case java.sql.Types.BOOLEAN:
					case java.sql.Types.BIT:
						upload.setBoolean(index + 1, (boolean) field);
						break;
					case java.sql.Types.CHAR:
					case java.sql.Types.NCHAR:
					case java.sql.Types.VARCHAR:
					case java.sql.Types.LONGVARCHAR:
					case java.sql.Types.LONGNVARCHAR:
						upload.setString(index + 1, (String) field);
						break;
					case java.sql.Types.TINYINT:
						upload.setByte(index + 1, (byte) field);
						break;
					case java.sql.Types.SMALLINT:
						upload.setShort(index + 1, (short) field);
						break;
					case java.sql.Types.INTEGER:
						upload.setInt(index + 1, (int) field);
						break;
					case java.sql.Types.BIGINT:
						upload.setLong(index + 1, (long) field);
						break;
					case java.sql.Types.REAL:
						upload.setFloat(index + 1, (float) field);
						break;
					case java.sql.Types.FLOAT:
					case java.sql.Types.DOUBLE:
						upload.setDouble(index + 1, (double) field);
						break;
					case java.sql.Types.DECIMAL:
					case java.sql.Types.NUMERIC:
						upload.setBigDecimal(index + 1, (java.math.BigDecimal) field);
						break;
					case java.sql.Types.DATE:
						upload.setDate(index + 1, (java.sql.Date) field);
						break;
					case java.sql.Types.TIME:
						upload.setTime(index + 1, (java.sql.Time) field);
						break;
					case java.sql.Types.TIMESTAMP:
						upload.setTimestamp(index + 1, (java.sql.Timestamp) field);
						break;
					case java.sql.Types.BINARY:
					case java.sql.Types.VARBINARY:
					case java.sql.Types.LONGVARBINARY:
						upload.setBytes(index + 1, (byte[]) field);
						break;
					case java.sql.Types.STRUCT: {
						Object[] objects = rowToObjectArray((Row) field);
						upload.setObject(index + 1, objects);

						break;
					}

					default:
						upload.setObject(index + 1, field);
						LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.",
							type, index + 1, field);
						// case java.sql.Types.SQLXML
						// case java.sql.Types.ARRAY:
						// case java.sql.Types.JAVA_OBJECT:
						// case java.sql.Types.BLOB:
						// case java.sql.Types.CLOB:
						// case java.sql.Types.NCLOB:
						// case java.sql.Types.DATALINK:
						// case java.sql.Types.DISTINCT:
						// case java.sql.Types.OTHER:
						// case java.sql.Types.REF:
						// case java.sql.Types.ROWID:
						// case java.sql.Types.STRUC
				}
			} catch (ClassCastException e) {
				// enrich the exception with detailed information.
				String errorMessage = String.format(
					"%s, field index: %s, field value: %s.", e.getMessage(), index, field);
				ClassCastException enrichedException = new ClassCastException(errorMessage);
				enrichedException.setStackTrace(e.getStackTrace());
				throw enrichedException;
			}
		}
	}

	public static Object getFieldFromResultSet(
		int index,
		int type,
		ResultSet set,
		TypeInformation<?> fieldType) throws SQLException {
		Object ret;
		switch (type) {
			case java.sql.Types.NULL:
				ret = null;
				break;
			case java.sql.Types.BOOLEAN:
			case java.sql.Types.BIT:
				ret = set.getBoolean(index + 1);
				break;
			case java.sql.Types.CHAR:
			case java.sql.Types.NCHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.LONGNVARCHAR:
				ret = set.getString(index + 1);
				break;
			case java.sql.Types.TINYINT:
				ret = set.getByte(index + 1);
				break;
			case java.sql.Types.SMALLINT:
				ret = set.getShort(index + 1);
				break;
			case java.sql.Types.INTEGER:
				ret = set.getInt(index + 1);
				break;
			case java.sql.Types.BIGINT:
				ret = set.getLong(index + 1);
				break;
			case java.sql.Types.REAL:
				ret = set.getFloat(index + 1);
				break;
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
				ret = set.getDouble(index + 1);
				break;
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
				ret = set.getBigDecimal(index + 1);
				break;
			case java.sql.Types.DATE:
				ret = set.getDate(index + 1);
				break;
			case java.sql.Types.TIME:
				ret = set.getTime(index + 1);
				break;
			case java.sql.Types.TIMESTAMP:
				ret = set.getTimestamp(index + 1);
				break;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				ret = set.getBytes(index + 1);
				break;
			case java.sql.Types.STRUCT:
				Object result = set.getObject(index + 1);
				ret = convert(result, fieldType);
				break;
			case java.sql.Types.ARRAY:
				Array array = set.getArray(index + 1);
				ret = convert(array, fieldType);
				break;
			default:
				ret = set.getObject(index + 1);
				LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to get its value: {}.",
					type, index + 1, ret);
				break;

			// case java.sql.Types.SQLXML
			// case java.sql.Types.ARRAY:
			// case java.sql.Types.JAVA_OBJECT:
			// case java.sql.Types.BLOB:
			// case java.sql.Types.CLOB:
			// case java.sql.Types.NCLOB:
			// case java.sql.Types.DATALINK:
			// case java.sql.Types.DISTINCT:
			// case java.sql.Types.OTHER:
			// case java.sql.Types.REF:
			// case java.sql.Types.ROWID:
			// case java.sql.Types.STRUC
		}

		if (set.wasNull()) {
			return null;
		} else {
			return ret;
		}
	}

	public static Row getPrimaryKey(Row row, int[] pkFields) {
		Row pkRow = new Row(pkFields.length);
		for (int i = 0; i < pkFields.length; i++) {
			pkRow.setField(i, row.getField(pkFields[i]));
		}
		return pkRow;
	}

	public static JdbcOptions getJdbcOptions(DescriptorProperties descriptorProperties) {
		final String driver = descriptorProperties.getString(CONNECTOR_DRIVER);
		final String url = descriptorProperties.getString(CONNECTOR_URL);

		final JdbcOptions.Builder builder = JdbcOptions.builder()
			.setDBUrl(url)
			.setDriverName(driver)
			.setTableName(descriptorProperties.getString(CONNECTOR_TABLE))
			.setDialect(JdbcDialects.get(url).orElse(CommonDialect.newInstance(driver)));

		descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
		descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);
		descriptorProperties.getOptionalBoolean(CONNECTOR_QUOTING).ifPresent(builder::setIsQuoting);

		return builder.build();
	}

	public static boolean isLinkoopdb(String driverName) {
		return "com.datapps.linkoopdb.jdbc.JdbcDriver".equals(driverName);
	}

	private static Object convert(Object item, TypeInformation<?> info) {
		if (item == null) {
			return null;
		}

		int type = JdbcTypeUtil.typeInformationToSqlType(info);
		Object ret;

		switch (type) {
			case java.sql.Types.NULL:
				ret = null;
				break;
			case java.sql.Types.BOOLEAN:
			case java.sql.Types.BIT:
				ret = Boolean.valueOf(item.toString());
				break;
			case java.sql.Types.CHAR:
			case java.sql.Types.NCHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.LONGNVARCHAR:
				ret = item.toString();
				break;
			case java.sql.Types.TINYINT:
				ret = Byte.valueOf(item.toString());
				break;
			case java.sql.Types.SMALLINT:
				ret = Short.valueOf(item.toString());
				break;
			case java.sql.Types.INTEGER:
				ret = Integer.valueOf(item.toString());
				break;
			case java.sql.Types.BIGINT:
				ret = Long.valueOf(item.toString());
				break;
			case java.sql.Types.REAL:
				ret = Float.valueOf(item.toString());
				break;
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
				ret = Double.valueOf(item.toString());
				break;
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
				ret = getBigDecimal(item);
				break;
			case java.sql.Types.DATE:
				ret = Date.valueOf(item.toString());
				break;
			case java.sql.Types.TIME: {
				TemporalAccessor parsedTime = SIMPLE_TIME_FORMAT.parse(item.toString());

				ZoneOffset zoneOffset = parsedTime.query(TemporalQueries.offset());
				LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

				if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0 || localTime.getNano() != 0) {
					throw new IllegalStateException("Invalid time format " + item.toString());
				}

				ret = Time.valueOf(localTime);
				break;
			}

			case java.sql.Types.TIMESTAMP: {
				TemporalAccessor parsedTimestamp = SIMPLE_TIMESTAMP_FORMAT.parse(item.toString());

				ZoneOffset zoneOffset = parsedTimestamp.query(TemporalQueries.offset());

				if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0) {
					throw new IllegalStateException("Invalid timestamp format " + item.toString());
				}

				LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
				LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

				ret = Timestamp.valueOf(LocalDateTime.of(localDate, localTime));
				break;
			}

			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				ret = convertObjectArray(item, ObjectArrayTypeInfo.getInfoFor(BYTE_TYPE_INFO));
				break;
			case java.sql.Types.STRUCT: {
				Row input;
				//linkoop db udt
				if (item instanceof Object[]) {
					input = Row.of(((Object[]) item));
				} else {
					input = (Row) item;
				}
				TypeInformation<?>[] types = ((RowTypeInfo) info).getFieldTypes();
				Row row = new Row(types.length);
				for (int i = 0; i < types.length; i++) {
					row.setField(i, convert(input.getField(i), types[i]));
				}
				ret = row;

				break;
			}

			case java.sql.Types.ARRAY: {
				if (info instanceof ObjectArrayTypeInfo) {
					ret =  convertObjectArray(item, ((ObjectArrayTypeInfo<?, ?>) info).getComponentInfo());
				} else if (info instanceof BasicArrayTypeInfo) {
					ret =  convertObjectArray(item, ((BasicArrayTypeInfo<?, ?>) info).getComponentInfo());
				} else if (info instanceof PrimitiveArrayTypeInfo) {
					ret = convertObjectArray(item, ((PrimitiveArrayTypeInfo<?>) info).getComponentType());
				} else {
					throw new IllegalStateException(String.format("Invalid data type %s", info.toString()));
				}

				break;
			}

			default:
				throw new IllegalStateException("Invalid data type " + info.toString());

			// case java.sql.Types.SQLXML
			// case java.sql.Types.ARRAY:
			// case java.sql.Types.JAVA_OBJECT:
			// case java.sql.Types.BLOB:
			// case java.sql.Types.CLOB:
			// case java.sql.Types.NCLOB:
			// case java.sql.Types.DATALINK:
			// case java.sql.Types.DISTINCT:
			// case java.sql.Types.OTHER:
			// case java.sql.Types.REF:
			// case java.sql.Types.ROWID:
			// case java.sql.Types.STRUCT
		}

		return ret;
	}

	private static Object convertObjectArray(Object item, TypeInformation<?> elementType) {
		if (item.getClass().isArray()) {
			int length = java.lang.reflect.Array.getLength(item);
			final Object[] array = (Object[]) java.lang.reflect.Array.newInstance(elementType.getTypeClass(), length);
			for (int i = 0; i < length; i++) {
				array[i] = convert(java.lang.reflect.Array.get(item, i), elementType);
			}
			return array;
		} else if (item instanceof Array) {
			try {
				return convertObjectArray(((Array) item).getArray(), elementType);
			} catch (SQLException e) {
				throw new RuntimeException("JdbcUtils#convertObjectArray#Array", e);
			}
		} else {
			throw new RuntimeException(String.format("object %s is not a array!", item));
		}
	}

	private static BigDecimal getBigDecimal(Object value) {
		BigDecimal ret;
		if (value instanceof BigDecimal) {
			ret = (BigDecimal) value;
		} else if (value instanceof String) {
			ret = new BigDecimal((String) value);
		} else if (value instanceof BigInteger) {
			ret = new BigDecimal((BigInteger) value);
		} else if (value instanceof Number) {
			ret = BigDecimal.valueOf(((Number) value).doubleValue());
		} else {
			throw new ClassCastException("Not possible to coerce ["
				+ value + "] from class " + value.getClass() + " into a BigDecimal.");
		}
		return ret;
	}

	private static Object[] rowToObjectArray(Row row) {
		int arity = row.getArity();
		Object[] objects = new Object[arity];
		for (int i = 0; i < arity; i++) {
			Object object = row.getField(i);
			if (object instanceof Row) {
				object = rowToObjectArray((Row) object);
			}
			objects[i] = object;
		}
		return objects;
	}
}
