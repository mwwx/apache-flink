package org.apache.flink.streaming.connectors.parquet;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.flink.formats.parquet.ParquetInputFormat.readMessageType;

/**
 * parquet table source.
 */
public class ParquetTableSource implements
	/*ProjectableTableSource<Row>,*/ StreamTableSource<Row>, BatchTableSource<Row>,
	DefinedProctimeAttribute, DefinedRowtimeAttributes/*, DefinedFieldMapping*/ {

	// path
	private String path;
	// schema
	private TableSchema tableSchema;
	private TableSchema originTableSchema;

	private String proctimeAttribute;
	private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	public ParquetTableSource(String path, String[] fieldNames, TypeInformation[] fieldTypes,
		TableSchema originTableSchema, String proctimeAttribute,
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
		this.path = path;
		this.tableSchema = new TableSchema(fieldNames, fieldTypes);
		this.originTableSchema = originTableSchema;
		this.proctimeAttribute = proctimeAttribute;
		this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(originTableSchema.getFieldTypes(), originTableSchema.getFieldNames());
	}

	@Override
	public TableSchema getTableSchema() {
		return this.tableSchema;
	}

	@Override
	public String explainSource() {
		return TableConnectorUtil
			.generateRuntimeName(getClass(), getTableSchema().getFieldNames());
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(createInputFormat(), getReturnType()).name(explainSource());
	}

	private ParquetRowInputFormat createInputFormat() {
		try {
			ParquetRowInputFormat inputFormat = new ParquetRowInputFormat(new Path(path),
				readMessageType(new Path(path)));
			Configuration configuration = new Configuration();
			configuration.setBoolean(FileInputFormat.ENUMERATE_NESTED_FILES_FLAG, true);
			inputFormat.configure(configuration);
			return inputFormat;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getProctimeAttribute() {
		return proctimeAttribute;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return rowtimeAttributeDescriptors;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return ParquetSourceUtil.createInput(execEnv, createInputFormat(), getReturnType()).name(explainSource());
	}

	/**
	 * ParquetTableSourceBuilder.
	 */
	public static class ParquetTableSourceBuilder {
		private String path;
		private String[] fieldNames;
		private TypeInformation[] fieldTypes;
		private TableSchema originTableSchema;
		private String proctimeAttribute;
		private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

		public ParquetTableSource build() {
			if (path == null) {
				throw new IllegalArgumentException("Path must be defined.");
			}
			if (fieldNames == null || fieldNames.length < 1) {
				throw new IllegalArgumentException("Fields can not be empty.");
			}

			return new ParquetTableSource(
				path,
				fieldNames,
				fieldTypes,
				originTableSchema,
				proctimeAttribute,
				rowtimeAttributeDescriptors);
		}

		public ParquetTableSourceBuilder path(String path) {
			this.path = path;
			return this;
		}

		public ParquetTableSourceBuilder fields(String[] fieldNames, TypeInformation[] fieldTypes) {
			if (fieldNames.length != fieldTypes.length) {
				throw new IllegalArgumentException("Number of field names and field types must be equal.");
			}
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
			return this;
		}

		public ParquetTableSourceBuilder proctimeAttribute(String proctimeAttribute) {
			this.proctimeAttribute = proctimeAttribute;
			return this;
		}

		public ParquetTableSourceBuilder rowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
			this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
			return this;
		}

		public ParquetTableSourceBuilder originTableSchema(TableSchema originTableSchema) {
			this.originTableSchema = originTableSchema;
			return this;
		}

	}
}
