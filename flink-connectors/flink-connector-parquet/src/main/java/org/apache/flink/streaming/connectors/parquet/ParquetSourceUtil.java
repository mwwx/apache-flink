package org.apache.flink.streaming.connectors.parquet;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Preconditions;

/**
 * ParquetSourceUtil.
 */
public class ParquetSourceUtil {

	public static <OUT> DataStreamSource<OUT> createInput(StreamExecutionEnvironment environment, InputFormat<OUT, ?> inputFormat, TypeInformation<OUT> typeInfo) {
		DataStreamSource<OUT> source;

		@SuppressWarnings("unchecked")
		FileInputFormat<OUT> format = (FileInputFormat<OUT>) inputFormat;

		source = createLoopFileInput(environment, format, typeInfo, "loop Custom File source",
			FileProcessingMode.PROCESS_CONTINUOUSLY, 100);
		return source;
	}

	private static  <OUT> DataStreamSource<OUT> createLoopFileInput(StreamExecutionEnvironment environment, FileInputFormat<OUT> inputFormat,
		TypeInformation<OUT> typeInfo,
		String sourceName,
		FileProcessingMode monitoringMode,
		long interval) {

		Preconditions.checkNotNull(inputFormat, "Unspecified file input format.");
		Preconditions.checkNotNull(typeInfo, "Unspecified output type information.");
		Preconditions.checkNotNull(sourceName, "Unspecified name for the source.");
		Preconditions.checkNotNull(monitoringMode, "Unspecified monitoring mode.");

		Preconditions.checkArgument(monitoringMode.equals(FileProcessingMode.PROCESS_ONCE) ||
				interval >= ContinuousFileMonitoringFunction.MIN_MONITORING_INTERVAL,
			"The path monitoring interval cannot be less than " +
				ContinuousFileMonitoringFunction.MIN_MONITORING_INTERVAL + " ms.");

		ContinuousFileMonitoringFunction<OUT> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(inputFormat, monitoringMode, environment.getParallelism(), interval);

		ContinuousFileReaderOperator<OUT> reader =
			new ContinuousFileReaderOperator<>(inputFormat);

		SingleOutputStreamOperator<OUT> source = environment.addSource(monitoringFunction, sourceName)
			.transform("Split Reader: " + sourceName, typeInfo, reader);

		return new DataStreamSource<>(source);
	}
}
