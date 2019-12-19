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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.JdbcDataFetcher;
import org.apache.flink.connector.jdbc.internal.JdbcSourceFunction;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcReadOptions;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.lookup.AsyncLookupTableFunction;
import org.apache.flink.table.sources.lookup.LookupOptions;
import org.apache.flink.table.sources.lookup.SyncLookupTableFunction;
import org.apache.flink.table.sources.lookup.cache.CacheStrategy;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link TableSource} for JDBC.
 */
public class JdbcTableSource implements
	StreamTableSource<Row>,
	ProjectableTableSource<Row>,
	LookupableTableSource<Row>,
	DefinedProctimeAttribute,
	DefinedRowtimeAttributes,
	DefinedFieldMapping {

	private final JdbcOptions options;
	private final JdbcReadOptions readOptions;
	private final LookupOptions lookupOptions;
	private final TableSchema schema;
	private final TableSchema originSchema;

	// index of fields selected, null means that all fields are selected
	private final int[] selectFields;

	/** Field name of the processing time attribute, null if no processing time field is defined. */
	private String proctimeAttribute;

	/** Descriptor for a rowtime attribute. */
	private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	/** Mapping for the fields of the table schema to fields of the physical returned type. */
	private Map<String, String> fieldMapping;

	private final DataType producedDataType;

	private JdbcTableSource(
		JdbcOptions options, JdbcReadOptions readOptions, LookupOptions lookupOptions, TableSchema schema,
		TableSchema originSchema, Map<String, String> fieldMapping,
		String proctimeAttribute, List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
		this(options, readOptions, lookupOptions, schema,
			originSchema, fieldMapping, proctimeAttribute,
			rowtimeAttributeDescriptors, null);
	}

	private JdbcTableSource(
		JdbcOptions options, JdbcReadOptions readOptions, LookupOptions lookupOptions,
		TableSchema schema, TableSchema originSchema, Map<String, String> fieldMapping, String proctimeAttribute,
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors, int[] selectFields) {
		this.options = options;
		this.readOptions = readOptions;
		this.lookupOptions = lookupOptions;
		this.schema = schema;
		this.originSchema = originSchema;
		this.fieldMapping = fieldMapping;

		this.proctimeAttribute = proctimeAttribute;
		this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
		this.selectFields = selectFields;

		final DataType[] schemaDataTypes = originSchema.getFieldDataTypes();
		final String[] schemaFieldNames = originSchema.getFieldNames();
		if (selectFields != null) {
			DataType[] dataTypes = new DataType[selectFields.length];
			String[] fieldNames = new String[selectFields.length];
			for (int i = 0; i < selectFields.length; i++) {
				dataTypes[i] = schemaDataTypes[selectFields[i]];
				fieldNames[i] = schemaFieldNames[selectFields[i]];
			}
			this.producedDataType =
					TableSchema.builder().fields(fieldNames, dataTypes).build().toRowDataType();
		} else {
			this.producedDataType = originSchema.toRowDataType();
		}
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		RowTypeInfo typeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
		if (readOptions.getIncreaseColumn().isPresent()) {
			return execEnv.addSource(new JdbcSourceFunction(getInputFormat(), typeInfo.getArity()))
				.returns((TypeInformation) typeInfo)
				.name(explainSource());
		} else {
			return execEnv.createInput(
				getInputFormat(),
				typeInfo)
				.name(explainSource());
		}
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		final RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
		return new SyncLookupTableFunction(lookupOptions,
			new JdbcDataFetcher(
				options,
				lookupOptions,
				rowTypeInfo.getFieldNames(),
				rowTypeInfo.getFieldTypes(),
				lookupKeys,
				readOptions.getFetchSize()));
	}

	@Override
	public DataType getProducedDataType() {
		return producedDataType;
	}

	@Override
	public TableSource<Row> projectFields(int[] fields) {
		return new JdbcTableSource(
			options, readOptions, lookupOptions,
			schema, originSchema, fieldMapping,
			proctimeAttribute, rowtimeAttributeDescriptors, fields);
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(getProducedDataType());
		return new AsyncLookupTableFunction(lookupOptions,
			new JdbcDataFetcher(
				options,
				lookupOptions,
				rowTypeInfo.getFieldNames(),
				rowTypeInfo.getFieldTypes(),
				lookupKeys,
				readOptions.getFetchSize()));
	}

	@Override
	public boolean isAsyncEnabled() {
		return lookupOptions.isAsyncEnabled();
	}

	@Override
	public CacheStrategy[] supportedCacheStrategies() {
		return new CacheStrategy[] {CacheStrategy.ALL, CacheStrategy.LRU, CacheStrategy.NONE};
	}

	@Override
	public LookupOptions getLookupOptions() {
		return lookupOptions;
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Nullable
	@Override
	public String getProctimeAttribute() {
		return proctimeAttribute;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return rowtimeAttributeDescriptors;
	}

	@Nullable
	@Override
	public Map<String, String> getFieldMapping() {
		return fieldMapping;
	}

	@Override
	public String explainSource() {
		final RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
		return TableConnectorUtils.generateRuntimeName(getClass(), rowTypeInfo.getFieldNames());
	}

	public static Builder builder() {
		return new Builder();
	}

	private JdbcInputFormat getInputFormat() {
		final RowTypeInfo rowTypeInfo = (RowTypeInfo) fromDataTypeToLegacyInfo(producedDataType);
		JdbcInputFormat.JdbcInputFormatBuilder builder = JdbcInputFormat.buildJdbcInputFormat()
				.setDrivername(options.getDriverName())
				.setDBUrl(options.getDbURL())
				.setIncreaseColumn(readOptions.getIncreaseColumn().isPresent()
					? readOptions.getIncreaseColumn().get() : null)
				.setRowTypeInfo(new RowTypeInfo(rowTypeInfo.getFieldTypes(), rowTypeInfo.getFieldNames()));
		options.getUsername().ifPresent(builder::setUsername);
		options.getPassword().ifPresent(builder::setPassword);

		if (readOptions.getFetchSize() != 0) {
			builder.setFetchSize(readOptions.getFetchSize());
		}

		final JdbcDialect dialect = options.getDialect();
		String query = getBaseQueryStatement(rowTypeInfo);
		if (readOptions.getPartitionColumnName().isPresent()) {
			long lowerBound = readOptions.getPartitionLowerBound().get();
			long upperBound = readOptions.getPartitionUpperBound().get();
			int numPartitions = readOptions.getNumPartitions().get();
			builder.setParametersProvider(
				new JdbcNumericBetweenParametersProvider(lowerBound, upperBound).ofBatchNum(numPartitions));
			query += " WHERE " +
				dialect.quoteIdentifier(readOptions.getPartitionColumnName().get()) +
				" BETWEEN ? AND ?";
		}
		builder.setQuery(query);

		return builder.finish();
	}

	private String getBaseQueryStatement(RowTypeInfo rowTypeInfo) {
		return readOptions.getQuery().orElseGet(() ->
			options.getDialect().getSelectFromStatement(
				options.getTableName(), rowTypeInfo.getFieldNames(), new String[0]));
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JdbcTableSource) {
			JdbcTableSource source = (JdbcTableSource) o;
			return Objects.equals(options, source.options)
				&& Objects.equals(readOptions, source.readOptions)
				&& Objects.equals(lookupOptions, source.lookupOptions)
				&& Objects.equals(schema, source.schema)
				&& Objects.equals(originSchema, source.originSchema)
				&& Arrays.equals(selectFields, source.selectFields)
				&& Objects.equals(proctimeAttribute, source.proctimeAttribute)
				&& Objects.equals(rowtimeAttributeDescriptors, source.rowtimeAttributeDescriptors)
				&& Objects.equals(fieldMapping, source.fieldMapping);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(options, readOptions,
			lookupOptions, schema, originSchema, proctimeAttribute,
			rowtimeAttributeDescriptors, fieldMapping, producedDataType);
		result = 31 * result + Arrays.hashCode(selectFields);
		return result;
	}

	/**
	 * Builder for a {@link JdbcTableSource}.
	 */
	public static class Builder {

		private JdbcOptions options;
		private JdbcReadOptions readOptions;
		private LookupOptions lookupOptions;
		protected TableSchema schema;
		private TableSchema originSchema;
		private String proctimeAttribute;
		private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = Collections.emptyList();
		private Map<String, String> fieldMapping;

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JdbcOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, scan related options.
		 * {@link JdbcReadOptions} will be only used for {@link StreamTableSource}.
		 */
		public Builder setReadOptions(JdbcReadOptions readOptions) {
			this.readOptions = readOptions;
			return this;
		}

		/**
		 * optional, lookup related options.
		 * {@link LookupOptions} only be used for {@link LookupableTableSource}.
		 */
		public Builder setLookupOptions(LookupOptions lookupOptions) {
			this.lookupOptions = lookupOptions;
			return this;
		}

		/**
		 * required, table schema of this table source.
		 */
		public Builder setSchema(TableSchema schema) {
			this.schema = JdbcTypeUtil.normalizeTableSchema(schema);
			return this;
		}

		/**
		 * required, table schema of this table source.
		 */
		public Builder setOriginSchema(TableSchema schema) {
			this.originSchema = JdbcTypeUtil.normalizeTableSchema(schema);
			return this;
		}

		public Builder setProctimeAttribute(String proctimeAttribute) {
			this.proctimeAttribute = proctimeAttribute;
			return this;
		}

		public Builder setRowtimeAttributeDescriptors(
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
			this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
			return this;
		}

		public Builder setFieldMapping(Map<String, String> fieldMapping) {
			this.fieldMapping = fieldMapping;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JdbcTableSource
		 */
		public JdbcTableSource build() {
			checkNotNull(options, "No options supplied.");
			checkNotNull(schema, "No schema supplied.");
			if (readOptions == null) {
				readOptions = JdbcReadOptions.builder().build();
			}
			if (lookupOptions == null) {
				lookupOptions = LookupOptions.builder().build();
			}
			if (originSchema == null) {
				originSchema = schema;
			}
			return new JdbcTableSource(options, readOptions, lookupOptions, schema,
				originSchema, fieldMapping, proctimeAttribute, rowtimeAttributeDescriptors);
		}
	}
}
